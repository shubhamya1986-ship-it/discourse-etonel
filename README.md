# discourse-etonel

ETL and analytics pipeline for community engagement data from the [OpenAI Community Forum](https://community.openai.com/), which runs on Discourse.

The pipeline:

1. **Extracts** topic lists from three Discourse JSON endpoints.
2. **Transforms** them into one normalized, de-duplicated topic record.
3. **Loads** them into ClickHouse as append-only snapshots, with a "latest state per topic" view on top.
4. **Analyses** the data with SQL and a Jupyter notebook.

## Data sources

| Source | Endpoint |
|--------|----------|
| latest | https://community.openai.com/latest.json |
| top    | https://community.openai.com/top.json |
| hot    | https://community.openai.com/hot.json |

Each topic's canonical URL is `https://community.openai.com/t/{topic_slug}/{topic_id}`.

## Project structure

```
discourse-etonel/
├── schema.sql                # ClickHouse DDL (snapshot table + dimension view)
├── etl.py                    # Extract, transform, load
├── queries.sql               # SQL for the business scenarios
├── analysis_notebook.ipynb   # Visualizations
├── requirements.txt          # Python dependencies
└── README.md
```

## Setup

**Prerequisites:** Python 3.10+, and a running ClickHouse server (local Docker is the easiest).

1. **Start ClickHouse** (skip if you already have one):
   ```bash
   docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
   ```

2. **Create a virtual environment and install dependencies:**
   ```bash
   python -m venv .venv
   # Windows: .venv\Scripts\activate    macOS/Linux: source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure the connection** with environment variables (defaults shown):
   ```
   CLICKHOUSE_HOST=localhost
   CLICKHOUSE_PORT=8123
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=
   CLICKHOUSE_DATABASE=default
   ```
   Never commit real credentials. Keep them in a local `.env` file, which is git-ignored.

4. **Create the tables:** run the statements in `schema.sql` against ClickHouse (for example with `clickhouse-client --multiquery < schema.sql` or any SQL client).

5. **Run the ETL:**
   ```bash
   python etl.py
   ```

6. **Explore the data:** run the queries in `queries.sql`, or open the notebook:
   ```bash
   jupyter notebook analysis_notebook.ipynb
   ```

## Approach

### 1. Data modeling (`schema.sql`)

- **`topic_snapshots`** is the fact table. Every run appends one row per topic, so history is preserved.
  - Key fields: `topic_id`, `title`, `slug`, `url`, `views`, `posts_count`, `like_count`, `category_id`, `created_at`, `last_posted_at`, `sources`, `snapshot_ts`, `ingested_at`.
  - `sources` is an `Array(LowCardinality(String))` holding the lists a topic appeared in during that run (for example `['latest', 'hot']`).
  - Engine: `ReplacingMergeTree(ingested_at)`, ordered by `(snapshot_hour, topic_id)`, partitioned by month. The ordering key is what makes reruns idempotent (see below).
- **`dim_topics`** is a view returning the latest state per topic, using `argMax(column, snapshot_ts)` grouped by `topic_id`. Use it for "current" questions, and use the snapshot table for history and trends.
- Categories: the topic lists carry only `category_id`. For readable names, an optional `dim_categories` table can be loaded from the forum's categories endpoint. Without it, the category queries group by `category_id`.

### 2. ETL (`etl.py`)

- **Extract:** fetches all three endpoints with a shared `requests` session, a timeout, and retries with backoff on network errors and 5xx/429 responses. A single endpoint failing is logged and does not stop the other two.
- **Transform:** normalizes each topic into one flat record, builds the canonical URL, parses timestamps to UTC, and **merges duplicates**: a topic appearing in several lists becomes one record whose `sources` contains every list it appeared in. Numeric fields come from the freshest copy.
- **Load:** inserts the merged records in a single batch into `topic_snapshots`.
- **Idempotency:** each run stamps its rows with `snapshot_ts` and a `snapshot_hour` bucket. Because the table is a `ReplacingMergeTree` keyed on `(snapshot_hour, topic_id)`, running the ETL several times within the same hour converges to one row per topic rather than creating inconsistent duplicates. Queries that need strict de-duplication before background merges finish should use `FINAL` or the `dim_topics` view.
- Settings (connection details, request timeout, retry count) come from environment variables, and logging replaces `print`.

### 3. Business queries (`queries.sql`)

| # | Question | Approach |
|---|----------|----------|
| 1 | Top 20 topics by views | Order `dim_topics` by `views` descending, `LIMIT 20` |
| 2 | Velocity (posts per day since creation) | `posts_count / greatest(dateDiff('day', created_at, now()), 1)`, ordered descending |
| 3 | Trending in the last 24h | Snapshots where `has(sources, 'hot')` and `snapshot_ts >= now() - INTERVAL 24 HOUR` |
| 4 | Category analysis | Group `dim_topics` by `category_id`, sum posts and views |
| 5 | Creation patterns | Count topics by `toDayOfWeek(created_at)` and `toHour(created_at)` for heatmap data |

### 4. Visualization (`analysis_notebook.ipynb`)

The notebook connects to ClickHouse with `clickhouse-connect`, loads query results into pandas, and builds:

- **Bar chart:** top topics by views.
- **Scatter plot:** posts count vs. topic age, to show velocity.
- **Heatmap:** topic creation by hour and day of week.

## Tech stack

Python, `requests`, `clickhouse-connect`, `pandas`, `matplotlib` / `seaborn`, Jupyter, ClickHouse.

## Notes and assumptions

- Discourse topic lists are paginated. The ETL fetches the first page of each endpoint by default; the page count is configurable.
- Counts such as `views` and `posts_count` are point-in-time values, which is why snapshots are kept alongside the latest-state view.
- This repository contains the design and code only. No results or sample outputs are included.
