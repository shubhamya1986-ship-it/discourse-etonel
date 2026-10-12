-- =====================================================================
-- schema.sql : ClickHouse DDL for the Discourse (OpenAI Community) ETL
--
-- NOTE: statements are split on the semicolon character by etl.py
-- --init-schema, so do not put that character inside comments or strings.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS discourse;


-- 1. Snapshot (fact) table
CREATE TABLE IF NOT EXISTS discourse.topics_snapshot
(
    snapshot_bucket     DateTime('UTC')            COMMENT 'Run time floored to the bucket size: the idempotency key',
    snapshot_ts         DateTime64(3, 'UTC')       COMMENT 'Exact time the ETL run started',
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'Row insert time: ReplacingMergeTree version',
    run_id              UUID                       COMMENT 'Links to etl_runs.run_id',

    topic_id            UInt64,
    slug                String,
    title               String,
    url                 String                     COMMENT 'https://community.openai.com/t/{slug}/{id}',
    category_id         UInt32                     COMMENT '0 when missing',
    tags                Array(String),

    views               UInt32,
    posts_count         UInt32,
    reply_count         UInt32,
    like_count          UInt32,
    highest_post_number UInt32,

    created_at          DateTime64(3, 'UTC'),
    last_posted_at      Nullable(DateTime64(3, 'UTC')),
    bumped_at           Nullable(DateTime64(3, 'UTC')),

    pinned              UInt8,
    closed              UInt8,
    archived            UInt8,
    visible             UInt8,
    has_accepted_answer UInt8,

    sources             Array(LowCardinality(String)),
    in_latest           UInt8,
    in_top              UInt8,
    in_hot              UInt8,
    rank_latest         UInt16                     COMMENT '1-based position in the list, 0 = absent',
    rank_top            UInt16,
    rank_hot            UInt16
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_bucket)
ORDER BY (topic_id, snapshot_bucket)
SETTINGS non_replicated_deduplication_window = 1000;


-- 2. Dimension: latest state per topic
CREATE TABLE IF NOT EXISTS discourse.dim_topics
(
    topic_id            UInt64,
    slug                String,
    title               String,
    url                 String,
    category_id         UInt32,
    tags                Array(String),
    views               UInt32,
    posts_count         UInt32,
    reply_count         UInt32,
    like_count          UInt32,
    highest_post_number UInt32,
    created_at          DateTime64(3, 'UTC'),
    last_posted_at      Nullable(DateTime64(3, 'UTC')),
    bumped_at           Nullable(DateTime64(3, 'UTC')),
    pinned              UInt8,
    closed              UInt8,
    archived            UInt8,
    visible             UInt8,
    has_accepted_answer UInt8,
    last_sources        Array(LowCardinality(String)),
    snapshot_ts         DateTime64(3, 'UTC')       COMMENT 'Snapshot this state came from: version column'
)
ENGINE = ReplacingMergeTree(snapshot_ts)
ORDER BY topic_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS discourse.mv_dim_topics
TO discourse.dim_topics
AS
SELECT
    topic_id, slug, title, url, category_id, tags,
    views, posts_count, reply_count, like_count, highest_post_number,
    created_at, last_posted_at, bumped_at,
    pinned, closed, archived, visible, has_accepted_answer,
    sources AS last_sources,
    snapshot_ts
FROM discourse.topics_snapshot;


-- 3. Category lookup
CREATE TABLE IF NOT EXISTS discourse.dim_categories
(
    category_id        UInt32,
    name               String,
    slug               String,
    parent_category_id UInt32 DEFAULT 0,
    color              String,
    updated_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY category_id;


-- 4. ETL audit log
CREATE TABLE IF NOT EXISTS discourse.etl_runs
(
    run_id          UUID,
    started_at      DateTime64(3, 'UTC'),
    finished_at     DateTime64(3, 'UTC'),
    status          LowCardinality(String)         COMMENT 'success / partial / failed / dry_run',
    snapshot_bucket DateTime('UTC'),
    source_counts   Map(String, UInt32)            COMMENT 'raw topics fetched per endpoint',
    rows_fetched    UInt32                         COMMENT 'before de-duplication',
    rows_merged     UInt32                         COMMENT 'unique topics after merge',
    rows_inserted   UInt32                         COMMENT 'net new rows in topics_snapshot (0 = idempotent re-run)',
    error           String
)
ENGINE = MergeTree
ORDER BY started_at;


-- 5. Views
CREATE VIEW IF NOT EXISTS discourse.v_dim_topics AS
SELECT * FROM discourse.dim_topics FINAL;

CREATE VIEW IF NOT EXISTS discourse.v_snapshots AS
SELECT * FROM discourse.topics_snapshot FINAL;

CREATE VIEW IF NOT EXISTS discourse.v_dim_categories AS
SELECT * FROM discourse.dim_categories FINAL;