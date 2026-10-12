# Project documentation: Discourse ETL and Analytics

## 1. Purpose

The goal is to analyse engagement on the OpenAI Community Forum. The forum runs on
Discourse, which exposes topic lists as JSON. We built a small pipeline that:

1. **Extracts** topic lists from three endpoints (latest, top, hot)
2. **Transforms** them into a clean, consistent shape and merges duplicates
3. **Loads** them into ClickHouse, a database built for fast analytics
4. **Analyses** them with SQL queries and charts in a notebook

## 2. The flow in plain words

```
Forum  ->  Python script  ->  Clean-up  ->  ClickHouse  ->  Queries and charts
```

Think of it as copying notes from a shop shelf. The forum is the shelf, the Python script is
the person copying, the clean-up removes repeated entries, ClickHouse is the filing cabinet,
and the queries and charts are the questions you ask the cabinet.

## 3. What data we collected

Three forum endpoints were used:

| Endpoint | Meaning |
|---|---|
| `latest.json` | Most recently active topics |
| `top.json` | Topics with the most activity over a period |
| `hot.json` | Topics trending right now |

Result of the first run:

| Item | Count |
|---|---|
| Topics from latest | 90 |
| Topics from top | 150 |
| Topics from hot | 90 |
| Total entries fetched | 330 |
| Unique topics after merging | 274 |
| Duplicates merged | 56 |

Fields stored for each topic:

- Identity: topic id, slug, title, canonical URL (`https://community.openai.com/t/{slug}/{id}`)
- Classification: category id, tags
- Engagement numbers: views, posts, replies, likes, highest post number
- Dates: created, last posted, last bumped
- Flags: pinned, closed, archived, visible, has accepted answer
- Provenance: which lists the topic appeared in, and its rank in each

Example row: the topic "Welcome to community.openai.com!" had 46,652 views, and appeared in
both the hot and latest lists.

## 4. Tools used, why, and alternatives

| Tool | Why we used it | Alternatives |
|---|---|---|
| Python 3.12 | Simple, strong libraries for APIs and data | Node.js, Go |
| `requests` | Reliable HTTP calls with retry support | `httpx`, `aiohttp` (async) |
| `clickhouse-connect` | Official Python client for ClickHouse over HTTP | `clickhouse-driver` (native protocol) |
| ClickHouse | Required by the assessment, very fast for counting and grouping | DuckDB, PostgreSQL, BigQuery, Snowflake |
| Docker Compose | Runs the database without a manual install | Native install, ClickHouse Cloud |
| Virtual environment (`.venv`) | Keeps this project's packages separate from other projects | `uv`, `conda`, `poetry` |
| Jupyter + pandas + matplotlib | Standard tools for querying data and drawing charts | `plotly`, `seaborn`, Streamlit, Metabase, Grafana |
| Plain Python dataclasses | Easy to read and test | `pydantic`, `pandas` |
| cron or Windows Task Scheduler | Simple scheduling | Airflow, Dagster, Prefect |

## 5. Files and what each does

| File | Role |
|---|---|
| `docker-compose.yml` | Starts ClickHouse with port 8123 (HTTP) and 9000 (native) and a saved data volume |
| `requirements.txt` | Python packages |
| `schema.sql` | Creates the database, tables, materialized view and views |
| `etl.py` | The pipeline: fetch, clean, merge, load, log |
| `queries.sql` | Business questions in SQL |
| `analysis_notebook.ipynb` | Charts |
| `README.md` | Setup and run instructions |
| `PROJECT_DOCS.md` | This document |

## 6. Data model (schema.sql)

| Object | What it is | Why it exists |
|---|---|---|
| `topics_snapshot` | Fact table with one row per topic per hourly bucket | Keeps history so growth and trends can be measured |
| `dim_topics` | Latest state of each topic | Fast lookups of current numbers |
| `mv_dim_topics` | Materialized view that copies each snapshot row into `dim_topics` | Keeps the dimension up to date automatically |
| `dim_categories` | Category id to name | The topic payload only gives a number |
| `etl_runs` | Log of every run | Transparency and debugging |
| `v_dim_topics`, `v_snapshots`, `v_dim_categories` | Views that read with `FINAL` | Hide duplicate-handling so queries stay simple |

**Why ReplacingMergeTree:** ClickHouse does not enforce unique keys. This table engine keeps
one row per sorting key (the newest) when it merges data in the background, which gives us
"latest wins" behaviour.

**Why the sorting key is `(topic_id, snapshot_bucket)`:** it serves both access patterns:
the history of one topic, and one row per topic per run.

## 7. How the ETL works (etl.py)

**Extract**
- Calls each endpoint with a session that retries on 429 and 5xx errors with backoff
- Follows pagination (`more_topics_url`) up to `--max-pages` pages, with a short delay
- Fetches category names from `/categories.json` as a best-effort extra

**Transform**
- Each raw topic becomes a clean record with UTC timestamps and the canonical URL
- Tags are normalised whether they arrive as text or objects
- Broken records are skipped with a warning
- Duplicates (same topic in several lists) are merged: the newest copy supplies descriptive
  fields, counters take the highest value, sources are combined, and the best rank per list is kept

**Load**
- One batched insert into `topics_snapshot`
- Categories refreshed into `dim_categories`
- One audit row written into `etl_runs`

**Safe failure:** if any endpoint fails, the run aborts and writes nothing. Otherwise a missing
list would look like "this topic is no longer hot". The `--allow-partial` flag overrides this.

## 8. Idempotency (safe to run repeatedly)

Two layers protect against duplicates:

1. **Logical key.** Each run gets a bucket (the run time rounded down to the hour). Rows with
   the same topic and bucket collapse into one through ReplacingMergeTree.
2. **Insert token.** Each insert carries a token built from the bucket and the data content. If
   the data is unchanged, ClickHouse drops the repeated insert. `etl_runs.rows_inserted = 0`
   shows when that happened.

If the forum data changed between two runs in the same hour, the second run is accepted and the
newest values win in the latest-state view.

## 9. Run results observed

| Step | Result |
|---|---|
| Dry run | 90 latest, 150 top, 90 hot, 330 normalised, 274 unique, 56 merged |
| First real run with `--init-schema` | Schema applied, `status=success inserted=274` |
| Audit log check | Row showed 330 fetched, 274 merged, 274 inserted |

Re-run check: running `python etl.py` again within the same hour should show `rows_inserted`
of 0 (or a small number if forum data changed) and leave the topic count unchanged.

## 10. The SQL queries and why we ran them

All queries read from the views, so they always see de-duplicated data. They were run in the
ClickHouse web page at `http://localhost:8123/play`.

### Query 1: Engagement leaderboard (top 20 topics by views)
- **Business question:** which topics attract the most attention?
- **How:** sort the latest topic state by `views` and keep 20.
- **Why this view:** `v_dim_topics`, because only the current number of views matters.

### Query 2: Velocity (posts per day since creation)
- **Business question:** which topics generate conversation fastest, regardless of how many views
  they have?
- **How:** divide `posts_count` by the topic's age in days. The age is never less than one hour,
  so a brand-new topic does not produce an absurd rate. Topics with fewer than 5 posts are
  excluded to avoid noise.
- **Why it matters:** an old topic can have many posts simply because it has existed a long
  time. Posts per day compares topics fairly.
- **Extra version in `queries.sql` (2b):** measures recent growth between the first and last
  snapshot in the last 7 days. It needs several ETL runs, and shows what is accelerating now.

### Query 3: Trending (hot list in the last 24 hours)
- **Business question:** which topics were flagged as hot recently?
- **How:** filter the snapshot history for `in_hot = 1` in the last 24 hours, then group by topic.
  `hot_snapshots` counts how many runs saw it as hot, and `best_hot_rank` is its best position.
- **Why the snapshot view:** this question is about history, not just the current state.
- **Note:** results grow richer as the ETL runs more often.

### Query 4: Category analysis
- **Business question:** which categories drive the most posts and views?
- **How:** group topics by category, add up posts, views and likes, compute averages per topic
  and each category's share of the total. A join with `dim_categories` supplies readable names,
  and falls back to `Category #id` if a name is missing.
- **Caveat:** totals describe the collected topics, which are a sample of the forum.

### Query 5: Creation patterns (heatmap data)
- **Business question:** when do people start new topics?
- **How:** count topics by weekday (1 = Monday to 7 = Sunday) and hour of day (UTC).
- **Why:** this is the data behind the heatmap chart, and it helps decide when moderators or
  support staff are most needed. Two extra versions exist in `queries.sql`: topics per calendar
  day, and topics per hour for the last 72 hours.

### Bonus: audit query
- Lists the latest runs from `etl_runs` so you can confirm each run's status and row counts.

## 11. The notebook charts and why

The notebook was built in JupyterLab one cell at a time, connecting to ClickHouse with
`clickhouse-connect` and drawing with matplotlib.

| Chart | What it shows | Why this chart type |
|---|---|---|
| Top 20 topics by views (horizontal bar) | Which topics get the most views | Bars make ranking easy, and horizontal bars fit long titles |
| Posts vs. topic age (scatter, log scales) | Velocity: where each topic sits, with dashed lines for 1, 10 and 100 posts per day | A scatter shows two measures at once, and log scales handle the huge range of ages and counts |
| Weekday by hour heatmap | When topics are created | A heatmap makes busy and quiet times visible at a glance |
| Category bars (bonus) | Views and posts per category | Side-by-side bars compare the two measures |

## 12. Assumptions and limitations

- The endpoints return a sample (about 30 topics per page), so numbers describe the collected
  topics, not the entire forum.
- Views, posts and likes are running totals, so growth analysis needs repeated runs over time.
- Timestamps are in UTC. For India time (IST), add 5 hours 30 minutes.
- The code was tested through a dry run, a real load and the SQL queries on the local setup.
  It has not been load-tested or run against a production cluster.

## 13. Possible improvements

- Fetch full topic pages (`/t/{id}.json`) for accepted answers and per-post details
- Schedule the ETL hourly with Task Scheduler, cron, Airflow or Dagster, and alert on failures
- Add a retention rule (TTL) or hourly roll-up tables as history grows
- Add a dashboard tool such as Metabase or Grafana on top of the views
- Add automated tests for the transform logic

## 14. Glossary

| Term | Meaning |
|---|---|
| ETL | Extract, Transform, Load: copy data, tidy it, store it |
| Snapshot | A copy of the data taken at one moment in time |
| Dimension | A table holding the current description of something (here, topics) |
| Idempotent | Running it again gives the same result, with no duplicates |
| Materialized view | A view that automatically copies data into another table on every insert |
| Bucket | A time window (here, one hour) used to group runs |
| Velocity | How fast a topic gains posts |
| UTC | The global reference time zone |



Steps to run the project

1. Go to the project folder

bash
cd your-project-folder

2. Create and activate a virtual environment

bash
python -m venv .venv

# Windows
.venv\Scripts\activate
# Mac/Linux
source .venv/bin/activate

3. Install packages

bash
pip install -r requirements.txt

4. Start ClickHouse (Docker must be running)

bash
docker compose up -d

Check it's up: open http://localhost:8123/play (user default, password clickhouse).

5. Test without writing anything (dry run)

bash
python etl.py --dry-run

You should see counts like 90 latest, 150 top, 90 hot, 274 unique. If the flag name differs, run python etl.py --help to see the exact options.

6. First real run (creates the tables too)

bash
python etl.py --init-schema

Expected: status=success inserted=274.

7. Run again to prove it's safe

bash
python etl.py

Within the same hour, rows_inserted should be 0 (or very small). The topic count stays the same.

8. Ask questions

Paste queries from queries.sql into the web page at http://localhost:8123/play, or
Open the charts:
bash
jupyter lab

Then open analysis_notebook.ipynb and run the cells top to bottom.

9. Check run history

sql
SELECT * FROM discourse.etl_runs ORDER BY started_at DESC LIMIT 5;
If something breaks
Problem	Fix
Connection refused	Docker isn't running, or run docker compose up -d again
Table doesn't exist	Run with --init-schema once
Empty query results	Run python etl.py first to load data
Trending/velocity (2b) looks empty	Needs several runs over time

Stop everything: docker compose down (data is kept in the volume).