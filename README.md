# macro-lakehouse

## Overview

macro-lakehouse is an automated Databricks lakehouse that downloads daily US Treasury
par yield curve data, cleans and validates it, and turns it into analysis-ready
recession indicators such as the 10-year minus 2-year spread, which has turned
negative ("inverted") ahead of most US recessions.

The dataset looks simple but raises real data engineering problems, each solved
with a specific Databricks feature:

| Problem | Solution |
|---|---|
| New files arrive daily and must be loaded once | **Auto Loader** picks up only new CSV files |
| Wide, messy CSV headers ("1 Mo", "1.5 Month"), new maturities added over time | Header standardisation, **unpivot** to long format, schema evolution |
| Bad or missing values | **Data quality expectations** that drop bad rows or stop the pipeline |
| Published values can be revised | **SCD Type 2** history via AUTO CDC, enabling point-in-time queries |
| Reruns must be safe | Idempotent loads: reloading a year never creates duplicates |
| Failures must be visible | An automated **run report** built from the pipeline event log |

## Architecture

```mermaid
flowchart TD
    A[US Treasury<br/>daily yield curve CSV] -->|1. ingest task<br/>Python wheel| B[(Unity Catalog Volume<br/>landing/)]
    B -->|Auto Loader| C[bronze_treasury_yields<br/>raw, append-only]
    C -->|unpivot + expectations| D[treasury_yields_long<br/>temporary view]
    D -->|AUTO CDC · SCD Type 2| E[silver_treasury_yields_history<br/>full history]
    E --> F[silver_treasury_yields<br/>current values]
    F --> G[gold_yield_curve_daily<br/>spreads, inversion]
    F --> H[ops_silver_health<br/>freshness, duplicates]
    I[(Pipeline event log)] -->|3. report task| J[HTML run report<br/>reports/ volume]

    subgraph P[2. refresh_pipeline · Lakeflow Declarative Pipeline]
        C
        D
        E
        F
        G
        H
    end
```
A Databricks Job runs three tasks in order. Ingest downloads the CSVs into a Unity Catalog volume, refresh_pipeline runs the declarative pipeline from bronze to gold, and report reads the pipeline’s event log to produce an HTML run report. The report task uses run_if: ALL_DONE, so a report is produced even when the pipeline fails, which is exactly when you need it most.

## Tech stack

| Tool | What it does here | Why this choice |
|---|---|---|
| **Databricks (serverless)** | Runs the job, pipeline and Python tasks | No clusters to size or manage; pay only while it runs |
| **Lakeflow Declarative Pipelines** | Defines bronze → silver → gold as tables in Python | Declares *what* each table is; Databricks handles ordering, retries and incremental updates |
| **Auto Loader** | Loads new CSV files from the landing volume | Tracks which files it has already processed, so each file loads exactly once |
| **AUTO CDC (SCD Type 2)** | Keeps the full history of every yield value | Treasury can revise published numbers; history allows point-in-time queries |
| **Delta Lake** | Storage format for all tables | ACID transactions, time travel and safe MERGE operations |
| **Liquid clustering** | Organises silver history by date and maturity | Speeds up the most common filters without managing partitions |
| **Unity Catalog** | Catalog, schema, volumes and table permissions | One place for governance, lineage and access control |
| **Lakeflow Jobs** | Orchestrates ingest → pipeline → report | Task dependencies, plus `run_if: ALL_DONE` so the report runs even after a failure |
| **Databricks Asset Bundles** | Deploys the job, pipeline and catalog objects from YAML | Infrastructure as code: repeatable, reviewable deployments from Git |
| **Python + uv** | Ingestion and report code, packaged as a wheel | uv gives fast, reproducible installs and builds the wheel the job runs |
| **pytest + ruff** | Unit tests and linting | Catches bugs and style issues before anything is deployed |
| **matplotlib** | Charts in the HTML run report | Simple, dependency-light charting for a static report |

## Data source

**US Department of the Treasury: Daily Treasury Par Yield Curve Rates**
([treasury.gov](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve))

- **What it is:** the yield (%) on US government debt at each maturity, from 1 month to 30 years, published every business day.
- **Format:** one CSV per calendar year, one row per date, one column per maturity.
- **Access:** public, free, no API key.
- **Volume:** small (~250 rows per year), which keeps the focus on correctness rather than scale.

### How it is ingested

The `ingest` job task (`macro_lakehouse.entrypoints:ingest`) downloads every year from
`start_year` (a bundle variable, default 2024) to the current year, and writes each one
as `treasury_<year>.csv` into a run-specific folder in the Unity Catalog `landing` volume.

The download client (`sources/treasury.py`) is built to fail safely:

| Risk | How it's handled |
|---|---|
| Temporary network or server errors | Retries with exponential backoff on 429 and 5xx responses |
| Requests hanging | 30-second timeout |
| An HTML error page returned with status 200 | `validate_csv` checks that the header starts with `Date` before anything is written |
| Client errors such as 404 | Not retried, because retrying would not help |

### Quirks in the data

| Quirk | Example | How it's handled |
|---|---|---|
| Inconsistent headers | `1 Mo`, `1.5 Month`, `10 Yr` | `sanitize_column` turns them into safe names (`t_1_mo`, `t_1_5_month`) |
| Maturities change over time | The 1.5-month and 4-month tenors were added in later years | Auto Loader schema evolution (`addNewColumns`) plus unpivoting to long format, so new maturities need no code changes |
| Wide format | One column per maturity | Unpivoted to one row per (date, maturity) for analysis |
| Maturity is only in the header text | `t_10_yr` | `tenor_months` parses it into months (120); `tenor_label` makes `10Y` |
| Missing values | Some maturities are blank on some dates | Data quality expectations in silver (see Data quality) |
| Published values can be revised | A rate re-published with a correction | SCD Type 2 history in silver (see Data model) |

### Reruns are safe

Each run re-downloads whole years into a new folder, so bronze can receive the same
rows more than once. That is deliberate: bronze stays raw and append-only, and
de-duplication happens in silver, where AUTO CDC keys on (date, maturity). Reloading
a year never creates duplicate silver rows. If a value has changed, it is recorded as
a new version.

## Data model

The pipeline follows the medallion pattern. Each layer has one job.

| Layer | Table | Grain (one row per…) | Type | Purpose |
|---|---|---|---|---|
| Bronze | `bronze_treasury_yields` | CSV row (date) | Streaming table | Raw data exactly as received, all values as strings, plus file lineage |
| Silver | `treasury_yields_long` | date × maturity | Temporary view | Unpivoted, typed rows where row-level quality rules run |
| Silver | `silver_treasury_yields_history` | date × maturity × version | Streaming table (SCD Type 2) | Every published version of every value |
| Silver | `silver_treasury_yields` | date × maturity | Materialized view | The current value for each key |
| Gold | `gold_yield_curve_daily` | date | Materialized view | Key tenors, recession spreads and curve shape |
| Ops | `ops_silver_health` | one row | Materialized view | Table-level checks: row count, duplicates, freshness |

### Bronze: raw and append-only
Bronze keeps values as **strings** (`inferColumnTypes = false`), so nothing is lost or
changed on the way in. Headers are cleaned into Delta-safe names, and three lineage
columns record where each row came from: `_source_file`, `_file_modified_at` and
`_ingested_at`. Bronze is the audit trail. If silver logic changes, it can be rebuilt
from here.

### Silver: long format, typed, versioned
`to_long` unpivots the wide CSV (one column per maturity) into **one row per
(curve_date, maturity)**, with `maturity_months` (e.g. 120), `maturity_label` (e.g. `10Y`)
and `yield_pct` as a double. Blank values are dropped, because Treasury leaves a
maturity empty for dates before it existed.

**Why long format?** New maturities become new rows, not new columns, so no code or
schema change is needed downstream. It also makes filtering and aggregating by
maturity simple.

**SCD Type 2 history.** `silver_treasury_yields_history` is maintained by AUTO CDC:
- **Keys:** `curve_date`, `maturity_months`
- **Sequence:** `_file_modified_at` (later files win)
- **Tracked column:** `yield_pct`. Only a *changed* yield creates a new version, so
  reloading identical data adds nothing.

Each version carries `__START_AT` and `__END_AT`. The current table keeps the rows where
`__END_AT IS NULL`, and `as_of()` returns the data exactly as it was known at any past
moment:

```sql
-- What did we believe the curve was on 1 March 2025?
SELECT curve_date, maturity_label, yield_pct
FROM silver_treasury_yields_history
WHERE __START_AT <= TIMESTAMP '2025-03-01'
  AND (__END_AT IS NULL OR __END_AT > TIMESTAMP '2025-03-01');
```

That prevents **look-ahead bias**: a backtest only sees values that had been published
at the time, not later corrections.

### Gold: analysis-ready metrics
`gold_yield_curve_daily` has one row per date with:

| Column | Meaning |
|---|---|
| `y_3m`, `y_2y`, `y_10y`, `y_30y` | Yields at the key maturities |
| `spread_10y_2y` | 10-year minus 2-year, the classic recession indicator |
| `spread_10y_3m` | 10-year minus 3-month, the indicator the Federal Reserve watches |
| `curve_shape` | `inverted` (10y–2y < 0), `flat` (within ±0.10 pp) or `normal` |
| `change_1d_10y` | Day-on-day change in the 10-year yield |

The key maturities are picked out with **conditional aggregation**
(`MAX(CASE WHEN …)`) rather than `pivot()`, because a declarative pipeline needs to
know the output schema up front.

### Clustering
History is clustered by `(curve_date, maturity_months)`, and the current and gold
tables by `curve_date`, matching the most common query filters. Liquid clustering
does this without managing physical partitions.

## Data quality
## Orchestration
## Monitoring
## Testing
## Local setup
## Deployment
## Lessons learned
## What this demonstrates