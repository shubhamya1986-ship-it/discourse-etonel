# Discourse ETL & Analytics (OpenAI Community Forum -> ClickHouse)

A small end-to-end pipeline that extracts topic lists from the OpenAI Community Forum
(Discourse), normalises and de-duplicates them, loads them into ClickHouse as an
append-only snapshot table, and exposes analytics through SQL and a Jupyter notebook.

```
 latest.json ┐
 top.json    ├─> fetch (retry, pagination) ─> normalise ─> merge by topic_id ─┐
 hot.json    ┘                                                                │
                                                                              v
                   ┌──────────── topics_snapshot (ReplacingMergeTree) <───────┘
                   │                 │ materialized view
                   │                 v
                   │            dim_topics ──> v_dim_topics   (latest state per topic)
                   └──> v_snapshots (FINAL)                    (history, de-duplicated)
 categories.json ─────────────> dim_categories
 every run ───────────────────> etl_runs (audit log)
```

## Project layout

| File | Purpose |
|---|---|
| `schema.sql` | DDL: snapshot table, dimension table + materialized view, categories, run log, views |
| `etl.py` | Extract / transform / load script (CLI) |
| `queries.sql` | SQL for the five business scenarios, plus audit checks |
| `analysis_notebook.ipynb` | Bar chart, velocity scatter, creation heatmap, category bonus chart |
| `tests/test_etl.py` | Unit tests for the pure transform logic |
| `docker-compose.yml` | Local ClickHouse |
| `requirements.txt` | Python dependencies |

## Setup

Requires Python 3.10+ and Docker (or any reachable ClickHouse 23.x or newer).

```bash
# 1. ClickHouse
docker compose up -d

# 2. Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Connection settings (defaults match docker-compose.yml except the password)
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=clickhouse
export CLICKHOUSE_DATABASE=discourse

# 4. First run creates the schema, then loads data
python etl.py --init-schema

# 5. Later runs (schedule with cron / Airflow, for example hourly)
python etl.py

# 6. Explore
jupyter lab analysis_notebook.ipynb
```

Useful flags: `--dry-run` (fetch and transform only), `--sources latest,hot`,
`--max-pages 5`, `--top-period weekly`, `--bucket-minutes 60`, `--allow-partial`,
`--log-level DEBUG`. Every flag also has an environment-variable equivalent (see `parse_args`).

Run the unit tests (no network or database needed):

```bash
pytest -q
```

Example hourly cron entry:

```
0 * * * * cd /path/to/discourse-etl && .venv/bin/python etl.py >> etl.log 2>&1
```

## Design

### Data model (Part 1)

* **`topics_snapshot`** is the fact table. One row per `(topic_id, snapshot_bucket)`
  holding the topic attributes, cumulative engagement metrics (`views`, `posts_count`,
  `like_count`, ...), timestamps, and provenance: a `sources` array, `in_latest/in_top/in_hot`
  flags and the rank in each list. Keeping history lets us measure growth over time,
  not just totals.
* **`dim_topics`** is the "latest state per topic" dimension. A materialized view copies every
  snapshot row into a `ReplacingMergeTree(snapshot_ts)` table, so the newest snapshot wins.
  `v_dim_topics` reads it with `FINAL`, so consumers never see stale or duplicate rows.
* **`dim_categories`** maps category ids to names. The topic-list payload only contains the id,
  so the ETL also reads `/categories.json` (best effort: a failure there never fails the run).
* **`etl_runs`** records every execution (status, counts per source, net rows inserted, error).
* Engine choices: partitioned monthly by `snapshot_bucket`; sorting key `(topic_id, snapshot_bucket)`
  serves both "history of a topic" and "latest per topic" access patterns.
  `LowCardinality(String)` is used for the small `sources` vocabulary.

### ETL (Part 2)

* **Extract:** a `requests` session with retries and exponential backoff on 429/5xx (honouring
  `Retry-After`), a descriptive User-Agent, per-request timeouts, and page-by-page pagination
  (via `topic_list.more_topics_url`) up to `--max-pages` with a polite delay.
* **Transform:** `normalize_topic` maps the raw dict to an immutable `TopicRecord` (UTC timestamps,
  canonical URL `https://community.openai.com/t/{slug}/{id}`, tags normalised from either string
  or object form, malformed records skipped with a warning).
* **Merge duplicates:** `merge_records` collapses the same `topic_id` across lists (and across
  pages). Descriptive fields come from the most recently bumped copy, counters take the max
  (they only grow), sources are unioned, and the best rank per source is kept.
* **Load:** a single batched insert into `topics_snapshot`, then a category refresh and an audit row.
* **Fail-safe:** if any endpoint fails, the run aborts without writing, because a missing endpoint
  would silently turn its `in_*` flag to 0 (for example "no longer hot"). `--allow-partial`
  overrides this and marks the run `partial`.

### Idempotency

Running the script repeatedly must not corrupt or inflate the data. Two layers cooperate:

1. **Logical key.** Each run gets a `snapshot_bucket` (run time floored to 60 minutes by default).
   The table is a `ReplacingMergeTree` ordered by `(topic_id, snapshot_bucket)`, so multiple runs in the
   same bucket converge to one row per topic (the newest `ingested_at`). Reads go through
   `v_snapshots` / `v_dim_topics`, which use `FINAL`.
2. **Insert de-duplication.** The INSERT carries an `insert_deduplication_token` computed from the
   bucket and the *content* of the data (not the run id or wall clock). If the forum data is
   unchanged, an immediate re-run (or a client retry after a timeout) is dropped by ClickHouse
   itself (`non_replicated_deduplication_window` is enabled on the table). `etl_runs.rows_inserted = 0`
   shows when that happened.

Schema creation is idempotent too (`CREATE ... IF NOT EXISTS`).
Use a shorter `--bucket-minutes` for finer history, or a longer one to keep fewer snapshots.

### Queries (Part 3)

All queries are in `queries.sql`:

1. Top 20 topics by views from the latest-state dimension.
2. Velocity: lifetime posts/day since creation (age floored at one hour, minimum five posts),
   plus a "recent velocity" variant from snapshot deltas over the last 7 days.
3. Trending: topics seen in the hot list in the last 24 hours, with how many snapshots they
   appeared in and their best rank.
4. Category analysis: posts, views, likes and share of total per category (joined to names).
5. Creation patterns: weekday x hour heatmap data, plus per-day and per-hour (last 72h) counts.

### Notebook (Part 4)

`analysis_notebook.ipynb` connects with `clickhouse-connect` and renders: a horizontal bar chart of
top topics by views, a log-log scatter of posts vs. age (dot size/colour = views, with constant
posts/day reference lines), a weekday x hour creation heatmap, and a bonus category chart.

## Assumptions and limitations

* The list endpoints return a *sample* of the forum (roughly 30 topics per page), so aggregates describe
  the collected topics, not the whole forum. Increase `--max-pages` for more coverage, balanced against
  the forum's rate limits.
* `views`, `posts_count`, and `like_count` are cumulative counters; time-based growth requires multiple
  snapshots (so run the ETL on a schedule).
* Timestamps are stored and analysed in UTC.
* `top.json` defaults to Discourse's default period unless `--top-period` is given.
* The code was written as a design deliverable and has not been run against the live forum or a
  ClickHouse server. Expect to adjust details such as Discourse payload fields or ClickHouse version
  specific settings on the first real run.

## Possible extensions

* Fetch full topic JSON (`/t/{id}.json`) for accepted answers, OP details and per-post data.
* Orchestrate with Airflow/Dagster, and alert on `etl_runs.status != 'success'`.
* Add a TTL on `topics_snapshot` for retention, or a rollup table (hourly aggregates) for dashboards.
