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
## Data model
## Data quality
## Orchestration
## Monitoring
## Testing
## Local setup
## Deployment
## Lessons learned
## What this demonstrates