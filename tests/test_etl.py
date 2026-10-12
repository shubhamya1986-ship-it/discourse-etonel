"""Unit tests for the pure transform logic in etl.py (no network, no ClickHouse)."""
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import etl  # noqa: E402

BASE = "https://community.openai.com"


def raw_topic(**overrides):
    topic = {
        "id": 42,
        "title": "Hello world",
        "slug": "hello-world",
        "category_id": 7,
        "views": 100,
        "posts_count": 5,
        "reply_count": 4,
        "like_count": 9,
        "highest_post_number": 5,
        "created_at": "2025-01-02T03:04:05.678Z",
        "last_posted_at": "2025-01-03T00:00:00.000Z",
        "bumped_at": "2025-01-03T00:00:00.000Z",
        "tags": ["api", {"name": "gpt", "id": 1}],
    }
    topic.update(overrides)
    return topic


def test_parse_ts_handles_z_suffix_and_garbage():
    dt = etl.parse_ts("2025-01-02T03:04:05.678Z")
    assert dt.tzinfo is not None and dt.year == 2025 and dt.microsecond == 678000
    assert etl.parse_ts(None) is None
    assert etl.parse_ts("not a date") is None


def test_normalize_builds_canonical_url_and_tags():
    rec = etl.normalize_topic(raw_topic(), "latest", 3, BASE)
    assert rec.url == f"{BASE}/t/hello-world/42"
    assert rec.tags == ("api", "gpt")
    assert rec.ranks == (("latest", 3),)


def test_normalize_skips_malformed():
    assert etl.normalize_topic({"title": "no id"}, "latest", 1, BASE) is None
    assert etl.normalize_topic(raw_topic(created_at=None), "latest", 1, BASE) is None


def test_merge_unions_sources_and_keeps_best_rank_and_max_metrics():
    a = etl.normalize_topic(raw_topic(views=100), "latest", 10, BASE)
    b = etl.normalize_topic(raw_topic(views=120, like_count=12), "hot", 2, BASE)
    c = etl.normalize_topic(raw_topic(id=99, slug="other"), "top", 1, BASE)

    merged = etl.merge_records([a, b, c])
    assert set(merged) == {42, 99}

    rec = merged[42]
    assert rec.sources == frozenset({"latest", "hot"})
    assert rec.views == 120 and rec.like_count == 12
    assert rec.rank_for("latest") == 10 and rec.rank_for("hot") == 2 and rec.rank_for("top") == 0


def test_merge_same_source_twice_keeps_lowest_rank():
    a = etl.normalize_topic(raw_topic(), "latest", 30, BASE)
    b = etl.normalize_topic(raw_topic(), "latest", 5, BASE)
    assert etl.merge_records([a, b])[42].rank_for("latest") == 5


def test_floor_to_bucket():
    ts = datetime(2025, 1, 1, 10, 47, 12, tzinfo=timezone.utc)
    assert etl.floor_to_bucket(ts, 60) == datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert etl.floor_to_bucket(ts, 15) == datetime(2025, 1, 1, 10, 45, tzinfo=timezone.utc)


def test_row_matches_column_order_and_flags():
    rec = etl.merge_records([etl.normalize_topic(raw_topic(), "hot", 4, BASE)])[42]
    ts = datetime(2025, 1, 1, 10, 47, tzinfo=timezone.utc)
    row = rec.to_row(etl.floor_to_bucket(ts, 60), ts, uuid.uuid4())
    assert len(row) == len(etl.SNAPSHOT_COLUMNS)
    as_dict = dict(zip(etl.SNAPSHOT_COLUMNS, row))
    assert as_dict["in_hot"] == 1 and as_dict["in_latest"] == 0 and as_dict["rank_hot"] == 4
    assert as_dict["sources"] == ["hot"]


def test_payload_token_is_deterministic_and_content_sensitive():
    bucket = datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    r1 = list(etl.merge_records([etl.normalize_topic(raw_topic(), "latest", 1, BASE)]).values())
    r2 = list(etl.merge_records([etl.normalize_topic(raw_topic(), "latest", 1, BASE)]).values())
    r3 = list(etl.merge_records([etl.normalize_topic(raw_topic(views=101), "latest", 1, BASE)]).values())
    assert etl.payload_token(bucket, r1) == etl.payload_token(bucket, r2)
    assert etl.payload_token(bucket, r1) != etl.payload_token(bucket, r3)
