#!/usr/bin/env python3
"""
ETL: Discourse (OpenAI Community Forum) -> ClickHouse.

Pipeline
    extract    GET latest.json / top.json / hot.json (+ pagination, retries)
    transform  normalise raw topic dicts into TopicRecord objects and merge
               duplicates (a topic may be present in several lists)
    load       append one snapshot row per unique topic into
               discourse.topics_snapshot, refresh dim_categories, and write
               an audit row into etl_runs

Idempotency
    * Every run is assigned a snapshot_bucket (run time floored to
      --bucket-minutes, default 60). The snapshot table is a
      ReplacingMergeTree keyed on (topic_id, snapshot_bucket), so several runs
      inside the same bucket collapse to one logical row per topic.
    * The INSERT carries a deterministic insert_deduplication_token derived
      from the payload, so an identical re-run (or a client retry) is dropped
      by ClickHouse itself and does not even create duplicate parts.

Usage
    python etl.py --init-schema
    python etl.py
    python etl.py --dry-run --log-level DEBUG
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOG = logging.getLogger("discourse_etl")

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
SOURCES: dict[str, str] = {
    "latest": "/latest.json",
    "top": "/top.json",
    "hot": "/hot.json",
}

SNAPSHOT_COLUMNS = [
    "snapshot_bucket", "snapshot_ts", "run_id",
    "topic_id", "slug", "title", "url", "category_id", "tags",
    "views", "posts_count", "reply_count", "like_count", "highest_post_number",
    "created_at", "last_posted_at", "bumped_at",
    "pinned", "closed", "archived", "visible", "has_accepted_answer",
    "sources", "in_latest", "in_top", "in_hot",
    "rank_latest", "rank_top", "rank_hot",
]  # ingested_at is omitted on purpose: ClickHouse fills it with now64(3)

CATEGORY_COLUMNS = [
    "category_id", "name", "slug", "parent_category_id", "color", "updated_at",
]

RUN_COLUMNS = [
    "run_id", "started_at", "finished_at", "status", "snapshot_bucket",
    "source_counts", "rows_fetched", "rows_merged", "rows_inserted", "error",
]


@dataclass(frozen=True)
class Config:
    base_url: str
    sources: tuple[str, ...]
    max_pages: int
    page_delay: float
    timeout: float
    top_period: Optional[str]
    bucket_minutes: int
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    ch_database: str
    ch_secure: bool
    user_agent: str = "discourse-etl-assessment/1.0 (+analytics; contact: data-team)"


# --------------------------------------------------------------------------
# Domain model
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class TopicRecord:
    topic_id: int
    slug: str
    title: str
    url: str
    category_id: int
    tags: tuple[str, ...]
    views: int
    posts_count: int
    reply_count: int
    like_count: int
    highest_post_number: int
    created_at: datetime
    last_posted_at: Optional[datetime]
    bumped_at: Optional[datetime]
    pinned: bool
    closed: bool
    archived: bool
    visible: bool
    has_accepted_answer: bool
    sources: frozenset[str]
    ranks: tuple[tuple[str, int], ...]  # (source, 1-based rank) pairs

    def rank_for(self, source: str) -> int:
        return dict(self.ranks).get(source, 0)

    def to_row(self, bucket: datetime, snapshot_ts: datetime, run_id: uuid.UUID) -> list[Any]:
        """Return values in SNAPSHOT_COLUMNS order."""
        return [
            bucket, snapshot_ts, run_id,
            self.topic_id, self.slug, self.title, self.url, self.category_id,
            list(self.tags),
            self.views, self.posts_count, self.reply_count, self.like_count,
            self.highest_post_number,
            self.created_at, self.last_posted_at, self.bumped_at,
            int(self.pinned), int(self.closed), int(self.archived),
            int(self.visible), int(self.has_accepted_answer),
            sorted(self.sources),
            int("latest" in self.sources), int("top" in self.sources), int("hot" in self.sources),
            self.rank_for("latest"), self.rank_for("top"), self.rank_for("hot"),
        ]


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: Any) -> Optional[datetime]:
    """Parse Discourse ISO-8601 strings ('2024-05-01T12:34:56.789Z') to aware UTC."""
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        LOG.debug("Unparseable timestamp: %r", value)
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def as_uint(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def floor_to_bucket(ts: datetime, minutes: int) -> datetime:
    """Floor ts to a minutes-wide bucket (UTC)."""
    step = max(minutes, 1) * 60
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % step, tz=timezone.utc)


def _tag_names(raw_tags: Any) -> tuple[str, ...]:
    """Discourse returns tags either as ['a','b'] or [{'name': 'a', ...}]."""
    names = []
    for tag in raw_tags or []:
        name = tag if isinstance(tag, str) else (tag or {}).get("name")
        if name:
            names.append(str(name))
    return tuple(names)


# --------------------------------------------------------------------------
# Extract
# --------------------------------------------------------------------------
def build_session(cfg: Config) -> requests.Session:
    """HTTP session with retries / exponential backoff on 429 and 5xx."""
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": cfg.user_agent, "Accept": "application/json"})
    return session


def fetch_source(session: requests.Session, cfg: Config, name: str) -> list[tuple[int, dict]]:
    """
    Fetch one topic-list endpoint, following pagination up to cfg.max_pages.
    Returns [(rank, raw_topic_dict), ...] where rank is the 1-based position.
    """
    url = f"{cfg.base_url}{SOURCES[name]}"
    results: list[tuple[int, dict]] = []
    for page in range(cfg.max_pages):
        params: dict[str, Any] = {}
        if page:
            params["page"] = page
        if name == "top" and cfg.top_period:
            params["period"] = cfg.top_period

        LOG.debug("GET %s params=%s", url, params)
        resp = session.get(url, params=params or None, timeout=cfg.timeout)
        resp.raise_for_status()
        topic_list = (resp.json() or {}).get("topic_list") or {}
        topics = topic_list.get("topics") or []
        if not topics:
            break
        for raw in topics:
            results.append((len(results) + 1, raw))
        if not topic_list.get("more_topics_url"):
            break
        time.sleep(cfg.page_delay)  # be polite to the forum
    LOG.info("source=%s fetched=%d topics", name, len(results))
    return results


def fetch_categories(session: requests.Session, cfg: Config) -> list[dict]:
    """Best-effort category lookup. Failures are non-fatal."""
    url = f"{cfg.base_url}/categories.json"
    try:
        resp = session.get(url, params={"include_subcategories": "true"}, timeout=cfg.timeout)
        resp.raise_for_status()
        roots = ((resp.json() or {}).get("category_list") or {}).get("categories") or []
    except (requests.RequestException, ValueError) as exc:
        LOG.warning("Could not fetch categories (continuing without): %s", exc)
        return []

    flat: dict[int, dict] = {}

    def walk(nodes: Iterable[dict], parent_id: int = 0) -> None:
        for node in nodes:
            cid = as_uint(node.get("id"))
            if not cid:
                continue
            flat[cid] = {
                "category_id": cid,
                "name": node.get("name") or "",
                "slug": node.get("slug") or "",
                "parent_category_id": as_uint(node.get("parent_category_id")) or parent_id,
                "color": node.get("color") or "",
            }
            walk(node.get("subcategory_list") or [], parent_id=cid)

    walk(roots)
    return list(flat.values())


# --------------------------------------------------------------------------
# Transform
# --------------------------------------------------------------------------
def normalize_topic(raw: dict, source: str, rank: int, base_url: str) -> Optional[TopicRecord]:
    """Map a raw Discourse topic dict to a TopicRecord. Returns None if unusable."""
    topic_id = as_uint(raw.get("id"))
    created_at = parse_ts(raw.get("created_at"))
    if not topic_id or created_at is None:
        LOG.warning("Skipping malformed topic from %s: id=%r created_at=%r",
                    source, raw.get("id"), raw.get("created_at"))
        return None

    slug = str(raw.get("slug") or "topic")
    return TopicRecord(
        topic_id=topic_id,
        slug=slug,
        title=str(raw.get("title") or raw.get("fancy_title") or ""),
        url=f"{base_url}/t/{slug}/{topic_id}",
        category_id=as_uint(raw.get("category_id")),
        tags=_tag_names(raw.get("tags")),
        views=as_uint(raw.get("views")),
        posts_count=as_uint(raw.get("posts_count")),
        reply_count=as_uint(raw.get("reply_count")),
        like_count=as_uint(raw.get("like_count")),
        highest_post_number=as_uint(raw.get("highest_post_number")),
        created_at=created_at,
        last_posted_at=parse_ts(raw.get("last_posted_at")),
        bumped_at=parse_ts(raw.get("bumped_at")),
        pinned=bool(raw.get("pinned")),
        closed=bool(raw.get("closed")),
        archived=bool(raw.get("archived")),
        visible=bool(raw.get("visible", True)),
        has_accepted_answer=bool(raw.get("has_accepted_answer")),
        sources=frozenset({source}),
        ranks=((source, rank),),
    )


def _merge_pair(a: TopicRecord, b: TopicRecord) -> TopicRecord:
    """
    Merge two records of the same topic.
    * descriptive fields come from the more recently bumped record
    * cumulative counters take the max (they only ever grow)
    * provenance (sources / ranks) is unioned, best (lowest) rank wins
    """
    floor = datetime.min.replace(tzinfo=timezone.utc)
    newer, _ = sorted((a, b), key=lambda r: (r.bumped_at or floor, r.posts_count), reverse=True)

    ranks: dict[str, int] = {}
    for src, rank in (*a.ranks, *b.ranks):
        ranks[src] = min(rank, ranks[src]) if src in ranks else rank

    return dataclasses.replace(
        newer,
        views=max(a.views, b.views),
        posts_count=max(a.posts_count, b.posts_count),
        reply_count=max(a.reply_count, b.reply_count),
        like_count=max(a.like_count, b.like_count),
        highest_post_number=max(a.highest_post_number, b.highest_post_number),
        sources=a.sources | b.sources,
        ranks=tuple(sorted(ranks.items())),
    )


def merge_records(records: Iterable[TopicRecord]) -> dict[int, TopicRecord]:
    """De-duplicate by topic_id across (and within) sources."""
    merged: dict[int, TopicRecord] = {}
    for rec in records:
        current = merged.get(rec.topic_id)
        merged[rec.topic_id] = rec if current is None else _merge_pair(current, rec)
    return merged


def payload_token(bucket: datetime, records: Iterable[TopicRecord]) -> str:
    """
    Deterministic insert-deduplication token. Depends only on the bucket and the
    content of the data (not on run_id / wall clock), so re-running with
    unchanged forum data produces the same token and ClickHouse drops the insert.
    """
    digest_src = [
        (r.topic_id, r.views, r.posts_count, r.like_count,
         r.bumped_at.isoformat() if r.bumped_at else None, sorted(r.sources))
        for r in sorted(records, key=lambda r: r.topic_id)
    ]
    digest = hashlib.sha256(json.dumps(digest_src, default=str).encode()).hexdigest()[:32]
    return f"topics_snapshot:{int(bucket.timestamp())}:{digest}"


# --------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------
def get_client(cfg: Config):
    import clickhouse_connect  # imported lazily so pure functions stay testable

    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
        secure=cfg.ch_secure,
    )


def apply_schema(client, schema_path: Path) -> None:
    """Execute schema.sql statement by statement (idempotent: IF NOT EXISTS)."""
    text = re.sub(r"--[^\n]*", "", schema_path.read_text(encoding="utf-8"))
    for stmt in (s.strip() for s in text.split(";")):
        if stmt:
            client.command(stmt)
    LOG.info("Schema applied from %s", schema_path)


def count_bucket_rows(client, cfg: Config, bucket: datetime) -> int:
    """Raw (non-FINAL) row count of a bucket, used to report net inserts."""
    return int(client.command(
        f"SELECT count() FROM {cfg.ch_database}.topics_snapshot "
        "WHERE snapshot_bucket = toDateTime({b:UInt32})",  # epoch: no timezone ambiguity
        parameters={"b": int(bucket.timestamp())},
    ))


def load_snapshot(client, cfg: Config, rows: list[list[Any]], token: str) -> None:
    client.insert(
        f"{cfg.ch_database}.topics_snapshot",
        rows,
        column_names=SNAPSHOT_COLUMNS,
        settings={"insert_deduplication_token": token},
    )


def load_categories(client, cfg: Config, categories: list[dict], now: datetime) -> None:
    if not categories:
        return
    rows = [[c["category_id"], c["name"], c["slug"], c["parent_category_id"], c["color"], now]
            for c in categories]
    client.insert(f"{cfg.ch_database}.dim_categories", rows, column_names=CATEGORY_COLUMNS)


def log_run(client, cfg: Config, **fields: Any) -> None:
    """Write the audit row. Never raises: auditing must not mask the real outcome."""
    try:
        row = [fields[c] for c in RUN_COLUMNS]
        client.insert(f"{cfg.ch_database}.etl_runs", [row], column_names=RUN_COLUMNS)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Could not write etl_runs audit row: %s", exc)


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def run(cfg: Config, *, dry_run: bool, allow_partial: bool, init_schema: bool, schema_path: Path) -> int:
    run_id = uuid.uuid4()
    started = utcnow()
    bucket = floor_to_bucket(started, cfg.bucket_minutes)
    LOG.info("run_id=%s bucket=%s dry_run=%s", run_id, bucket.isoformat(), dry_run)

    client = None if dry_run else get_client(cfg)
    if init_schema and client is not None:
        apply_schema(client, schema_path)

    # ---- Extract -----------------------------------------------------
    session = build_session(cfg)
    normalized: list[TopicRecord] = []
    source_counts: dict[str, int] = {}
    errors: dict[str, str] = {}

    for name in cfg.sources:
        try:
            fetched = fetch_source(session, cfg, name)
        except (requests.RequestException, ValueError) as exc:
            LOG.error("source=%s failed: %s", name, exc)
            errors[name] = str(exc)
            continue
        source_counts[name] = len(fetched)
        for rank, raw in fetched:
            rec = normalize_topic(raw, name, rank, cfg.base_url)
            if rec is not None:
                normalized.append(rec)

    status = "success"
    if errors:
        status = "partial" if allow_partial else "failed"
    err_text = "; ".join(f"{k}: {v}" for k, v in errors.items())

    # A missing source would silently flip its in_* flag to 0, so by default
    # we refuse to write a snapshot that is knowingly incomplete.
    if status == "failed" or not normalized:
        status = "failed"
        err_text = err_text or "no topics fetched"
        LOG.error("Aborting without writing snapshot: %s", err_text)
        if client is not None:
            log_run(client, cfg, run_id=run_id, started_at=started, finished_at=utcnow(),
                    status=status, snapshot_bucket=bucket, source_counts=source_counts,
                    rows_fetched=len(normalized), rows_merged=0, rows_inserted=0, error=err_text)
        return 1

    # ---- Transform ---------------------------------------------------
    merged = merge_records(normalized)
    rows = [rec.to_row(bucket, started, run_id)
            for rec in sorted(merged.values(), key=lambda r: r.topic_id)]
    LOG.info("normalized=%d unique_topics=%d (duplicates merged=%d)",
             len(normalized), len(merged), len(normalized) - len(merged))

    if dry_run:
        LOG.info("Dry run: nothing written. Sample row: %s", rows[0] if rows else None)
        return 0

    # ---- Load --------------------------------------------------------
    try:
        before = count_bucket_rows(client, cfg, bucket)
        load_snapshot(client, cfg, rows, payload_token(bucket, merged.values()))
        after = count_bucket_rows(client, cfg, bucket)
        inserted = max(after - before, 0)
        if inserted == 0:
            LOG.info("Identical payload already loaded for this bucket: insert de-duplicated")
        load_categories(client, cfg, fetch_categories(session, cfg), started)
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Load failed")
        log_run(client, cfg, run_id=run_id, started_at=started, finished_at=utcnow(),
                status="failed", snapshot_bucket=bucket, source_counts=source_counts,
                rows_fetched=len(normalized), rows_merged=len(merged), rows_inserted=0,
                error=str(exc))
        return 1

    log_run(client, cfg, run_id=run_id, started_at=started, finished_at=utcnow(),
            status=status, snapshot_bucket=bucket, source_counts=source_counts,
            rows_fetched=len(normalized), rows_merged=len(merged), rows_inserted=inserted,
            error=err_text)
    LOG.info("Done: status=%s inserted=%d", status, inserted)
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(description="Discourse -> ClickHouse ETL")
    p.add_argument("--base-url", default=env("DISCOURSE_BASE_URL", "https://community.openai.com"))
    p.add_argument("--sources", default=",".join(SOURCES),
                   help="comma separated subset of: " + ",".join(SOURCES))
    p.add_argument("--max-pages", type=int, default=int(env("MAX_PAGES", "3")),
                   help="pages per endpoint (about 30 topics per page)")
    p.add_argument("--page-delay", type=float, default=float(env("PAGE_DELAY", "1.0")))
    p.add_argument("--timeout", type=float, default=float(env("HTTP_TIMEOUT", "30")))
    p.add_argument("--top-period", default=env("TOP_PERIOD"),
                   help="optional period for top.json: daily|weekly|monthly|quarterly|yearly|all")
    p.add_argument("--bucket-minutes", type=int, default=int(env("BUCKET_MINUTES", "60")))
    p.add_argument("--allow-partial", action="store_true",
                   help="write the snapshot even if some endpoints failed")
    p.add_argument("--dry-run", action="store_true", help="fetch + transform only, no ClickHouse writes")
    p.add_argument("--init-schema", action="store_true", help="apply schema.sql before loading")
    p.add_argument("--schema-path", default=str(Path(__file__).with_name("schema.sql")))
    p.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    p.add_argument("--ch-host", default=env("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--ch-port", type=int, default=int(env("CLICKHOUSE_PORT", "8123")))
    p.add_argument("--ch-user", default=env("CLICKHOUSE_USER", "default"))
    p.add_argument("--ch-password", default=env("CLICKHOUSE_PASSWORD", ""))
    p.add_argument("--ch-database", default=env("CLICKHOUSE_DATABASE", "discourse"))
    p.add_argument("--ch-secure", action="store_true", default=env("CLICKHOUSE_SECURE", "0") == "1")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    chosen = tuple(s.strip() for s in args.sources.split(",") if s.strip())
    unknown = [s for s in chosen if s not in SOURCES]
    if unknown:
        LOG.error("Unknown source(s): %s", unknown)
        return 2

    cfg = Config(
        base_url=args.base_url.rstrip("/"),
        sources=chosen,
        max_pages=max(args.max_pages, 1),
        page_delay=args.page_delay,
        timeout=args.timeout,
        top_period=args.top_period,
        bucket_minutes=args.bucket_minutes,
        ch_host=args.ch_host,
        ch_port=args.ch_port,
        ch_user=args.ch_user,
        ch_password=args.ch_password,
        ch_database=args.ch_database,
        ch_secure=args.ch_secure,
    )
    return run(cfg, dry_run=args.dry_run, allow_partial=args.allow_partial,
               init_schema=args.init_schema, schema_path=Path(args.schema_path))


if __name__ == "__main__":
    sys.exit(main())