-- ClickHouse schema for Discourse topic analytics
-- Run these statements in order (e.g. clickhouse-client --multiquery < schema.sql)

-- ---------------------------------------------------------------------------
-- 1. Fact table: one row per topic per snapshot (append-only history)
--
-- Idempotency: ReplacingMergeTree keeps the row with the highest `ingested_at`
-- for each ORDER BY key. The key is (snapshot_hour, topic_id), so re-running the
-- ETL within the same hour converges to a single row per topic instead of
-- piling up duplicates. Use FINAL (or the dim_topics view) when you need
-- strictly de-duplicated reads before background merges have run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS topic_snapshots
(
    snapshot_ts     DateTime('UTC')                    COMMENT 'Exact start time of the ETL run',
    snapshot_hour   DateTime('UTC')                    COMMENT 'snapshot_ts truncated to the hour (idempotency bucket)',
    topic_id        UInt64,
    slug            String,
    title           String,
    url             String                             COMMENT 'https://community.openai.com/t/{slug}/{topic_id}',
    views           UInt32,
    posts_count     UInt32,
    reply_count     UInt32,
    like_count      UInt32,
    category_id     UInt32                             COMMENT '0 when the topic has no category',
    created_at      DateTime('UTC'),
    last_posted_at  Nullable(DateTime('UTC')),
    bumped_at       Nullable(DateTime('UTC')),
    sources         Array(LowCardinality(String))      COMMENT 'Lists the topic appeared in this run: latest / top / hot',
    ingested_at     DateTime('UTC') DEFAULT now()      COMMENT 'Row version used by ReplacingMergeTree'
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_hour)
ORDER BY (snapshot_hour, topic_id);


-- ---------------------------------------------------------------------------
-- 2. Optional category dimension (id -> readable name), loaded best-effort by etl.py
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_categories
(
    category_id  UInt32,
    name         String,
    slug         String,
    ingested_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY category_id;


-- ---------------------------------------------------------------------------
-- 3. Dimension view: latest known state per topic
--    - scalar columns come from the most recent snapshot (argMax by snapshot_ts)
--    - `sources` is the latest snapshot's lists; `all_sources` is every list the
--      topic has ever appeared in
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dim_topics AS
SELECT
    topic_id,
    argMax(slug,           snapshot_ts) AS slug,
    argMax(title,          snapshot_ts) AS title,
    argMax(url,            snapshot_ts) AS url,
    argMax(views,          snapshot_ts) AS views,
    argMax(posts_count,    snapshot_ts) AS posts_count,
    argMax(reply_count,    snapshot_ts) AS reply_count,
    argMax(like_count,     snapshot_ts) AS like_count,
    argMax(category_id,    snapshot_ts) AS category_id,
    argMax(created_at,     snapshot_ts) AS created_at,
    argMax(last_posted_at, snapshot_ts) AS last_posted_at,
    argMax(bumped_at,      snapshot_ts) AS bumped_at,
    argMax(sources,        snapshot_ts) AS sources,
    groupUniqArrayArray(sources)        AS all_sources,
    min(snapshot_ts)                    AS first_seen_at,
    max(snapshot_ts)                    AS last_seen_at
FROM topic_snapshots
GROUP BY topic_id;
