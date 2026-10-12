-- =====================================================================
-- queries.sql : business scenarios (ClickHouse SQL)
-- discourse.v_dim_topics = latest known state of each topic
-- discourse.v_snapshots  = de-duplicated snapshot history
-- All timestamps are UTC.
-- =====================================================================


-- 1. Engagement leaderboard: top 20 topics by views
SELECT
    topic_id,
    title,
    url,
    views,
    posts_count,
    like_count,
    created_at
FROM discourse.v_dim_topics
ORDER BY views DESC, topic_id
LIMIT 20;


-- 2a. Velocity: posts per day since creation
-- Age is floored at 1 hour so brand-new topics do not produce absurd ratios.
SELECT
    topic_id,
    title,
    url,
    posts_count,
    views,
    created_at,
    round(dateDiff('second', created_at, now64(3)) / 86400, 2) AS age_days,
    round(posts_count / greatest(dateDiff('second', created_at, now64(3)) / 86400.0,
                                 1.0 / 24), 2)                 AS posts_per_day
FROM discourse.v_dim_topics
WHERE posts_count >= 5
ORDER BY posts_per_day DESC
LIMIT 20;


-- 2b. Velocity (recent): growth between first and last snapshot in last 7 days
SELECT
    topic_id,
    argMax(title, snapshot_ts)                                      AS latest_title,
    argMax(url, snapshot_ts)                                        AS latest_url,
    argMin(posts_count, snapshot_ts)                                AS posts_at_start,
    argMax(posts_count, snapshot_ts)                                AS posts_at_end,
    min(snapshot_ts)                                                AS first_seen_ts,
    max(snapshot_ts)                                                AS last_seen_ts,
    round((posts_at_end - posts_at_start)
          / (dateDiff('second', first_seen_ts, last_seen_ts) / 86400.0), 2) AS recent_posts_per_day
FROM discourse.v_snapshots
WHERE snapshot_ts >= now64(3) - INTERVAL 7 DAY
GROUP BY topic_id
HAVING last_seen_ts > first_seen_ts + INTERVAL 1 HOUR
ORDER BY recent_posts_per_day DESC
LIMIT 20;


-- 3. Trending: topics that appeared in the hot list in the last 24h
SELECT
    topic_id,
    argMax(title, snapshot_ts)                    AS latest_title,
    argMax(url, snapshot_ts)                      AS latest_url,
    count()                                       AS hot_snapshots,
    min(snapshot_ts)                              AS first_hot_at,
    max(snapshot_ts)                              AS last_hot_at,
    minIf(rank_hot, rank_hot > 0)                 AS best_hot_rank,
    argMax(views, snapshot_ts)                    AS latest_views,
    argMax(posts_count, snapshot_ts)              AS latest_posts
FROM discourse.v_snapshots
WHERE in_hot = 1
  AND snapshot_ts >= now64(3) - INTERVAL 24 HOUR
GROUP BY topic_id
ORDER BY hot_snapshots DESC, best_hot_rank ASC;


-- 4. Category analysis: which categories drive the most posts and views
SELECT
    t.category_id                                                    AS cat_id,
    coalesce(c.name, concat('Category #', toString(t.category_id)))  AS category_name,
    count()                                                          AS topics,
    sum(t.posts_count)                                               AS total_posts,
    sum(t.views)                                                     AS total_views,
    sum(t.like_count)                                                AS total_likes,
    round(sum(t.posts_count) / count(), 1)                           AS avg_posts_per_topic,
    round(sum(t.views) / count(), 1)                                 AS avg_views_per_topic,
    round(100 * sum(t.posts_count) / sum(sum(t.posts_count)) OVER (), 2) AS pct_of_posts,
    round(100 * sum(t.views)       / sum(sum(t.views))       OVER (), 2) AS pct_of_views
FROM discourse.v_dim_topics AS t
LEFT JOIN discourse.v_dim_categories AS c ON t.category_id = c.category_id
GROUP BY t.category_id, c.name
ORDER BY total_views DESC
SETTINGS join_use_nulls = 1;


-- 5a. Creation patterns: heatmap data (weekday x hour of day, UTC)
SELECT
    toDayOfWeek(created_at)                 AS day_of_week,
    dateName('weekday', created_at)         AS weekday_name,
    toHour(created_at)                      AS hour_of_day,
    count()                                 AS topics_created
FROM discourse.v_dim_topics
GROUP BY day_of_week, weekday_name, hour_of_day
ORDER BY day_of_week, hour_of_day;


-- 5b. Topics created per calendar day
SELECT
    toDate(created_at)  AS created_date,
    count()             AS topics_created
FROM discourse.v_dim_topics
GROUP BY created_date
ORDER BY created_date;


-- 5c. Topics created per hour, last 72 hours
SELECT
    toStartOfHour(created_at)  AS created_hour,
    count()                    AS topics_created
FROM discourse.v_dim_topics
WHERE created_at >= now64(3) - INTERVAL 72 HOUR
GROUP BY created_hour
ORDER BY created_hour;


-- Bonus: recent ETL runs (rows_inserted = 0 means an idempotent re-run)
SELECT run_id, started_at, status, source_counts, rows_fetched, rows_merged, rows_inserted, error
FROM discourse.etl_runs
ORDER BY started_at DESC
LIMIT 20;

