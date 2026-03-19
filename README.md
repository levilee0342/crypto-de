# Crypto Data Engineering Pipeline

An end-to-end batch data engineering project that ingests cryptocurrency market data from the CoinGecko API, stores raw and processed datasets in Parquet, loads a Postgres warehouse, runs partition-level quality checks, and builds daily analytics tables with Apache Airflow orchestration.

## What this project demonstrates

- API ingestion with Python and `requests`
- Raw and processed data lake layers in Parquet
- Batch ETL orchestration with Apache Airflow
- Dimensional warehouse loading into Postgres
- Partition-aware pipeline runs with `run_date`
- Snapshot-based warehousing with `snapshot_date`
- Post-load warehouse quality checks
- Idempotent fact loading with conflict handling
- Incremental daily aggregate analytics for downstream reporting
- Local development with Docker Compose

## Architecture

```text
             +----------------------+
             |   CoinGecko API      |
             | /coins/markets       |
             +----------+-----------+
                        |
                        | Extract
                        v
          +----------------------------------+
          | Raw Data Lake                    |
          | data_lake/raw/dt=YYYY-MM-DD/     |
          | crypto.parquet                   |
          +----------------+-----------------+
                           |
                           | Transform
                           v
          +----------------------------------+
          | Processed Data Lake              |
          | data_lake/processed/dt=.../      |
          | crypto_clean.parquet             |
          +----------------+-----------------+
                           |
                           | Load
                           v
          +----------------------------------+
          | Postgres Data Warehouse          |
          |                                  |
          | dim_coin                         |
          | - coin_id (PK)                   |
          | - symbol                         |
          |                                  |
          | fact_price                       |
          | - coin_id (FK)                   |
          | - price                          |
          | - volume                         |
          | - market_cap                     |
          | - timestamp                      |
          | - snapshot_date                  |
          +----------------+-----------------+
                           |
                           | Quality check
                           v
          +----------------------------------+
          | Data Quality Layer               |
          |                                  |
          | - row count check                |
          | - null check                     |
          | - duplicate check                |
          | - orphan FK check                |
          +----------------+-----------------+
                           |
                           | Build analytics
                           v
          +----------------------------------+
          | Analytics Layer                  |
          |                                  |
          | daily_coin_metrics               |
          | - coin_id                        |
          | - snapshot_date                  |
          | - avg_price / max / min          |
          | - avg_volume                     |
          | - avg_market_cap                 |
          |                                  |
          | daily_market_summary             |
          | - snapshot_date                  |
          | - total_coins                    |
          | - avg_price_all_coins            |
          | - total_volume                   |
          | - total_market_cap               |
          +----------------------------------+
```

## Tech stack

- Python
- Pandas
- Apache Airflow
- PostgreSQL
- SQLAlchemy
- Docker Compose
- Parquet data lake layout

## Pipeline flow

### 1. Extract

`pipeline/extract.py` calls the CoinGecko `/coins/markets` endpoint, adds:

- `timestamp`: UTC ingestion time
- `snapshot_date`: logical pipeline date from Airflow `run_date`

Then it writes raw API output to:

`data_lake/raw/dt=YYYY-MM-DD/crypto.parquet`

### 2. Transform

`pipeline/transform.py` selects and standardizes the core columns used by the warehouse:

- `coin_id`
- `symbol`
- `price`
- `volume`
- `market_cap`
- `timestamp`
- `snapshot_date`

The transformed dataset is written to:

`data_lake/processed/dt=YYYY-MM-DD/crypto_clean.parquet`

### 3. Load

`pipeline/load.py` loads data into a small dimensional warehouse in Postgres:

- `dim_coin` stores one row per coin
- `fact_price` stores one row per `coin_id` and `timestamp`

Loading behavior:

- Dimension rows are upserted with `ON CONFLICT DO UPDATE`
- Fact rows are deduplicated by `(coin_id, timestamp)`
- `snapshot_date` is stored in the fact table for partition-aware checks and analytics
- Repeat runs for the same snapshot do not create duplicate fact rows

### 4. Quality check

`pipeline/quality.py` validates only the current `run_date` partition after loading. The checks include:

- fact row count for the current `snapshot_date`
- null `coin_id`
- null `timestamp`
- duplicate `(coin_id, timestamp)` groups
- orphan fact rows without matching `dim_coin`

### 5. Build analytics

`pipeline/build_analytics.py` incrementally rebuilds analytics only for the current `run_date` partition and writes two aggregate tables:

- `daily_coin_metrics`
- `daily_market_summary`

These tables support simple reporting and SQL-based analysis on top of the warehouse.

### 6. Orchestration

`dags/crypto_platform_daily_dag.py` defines a daily Airflow DAG:

`extract -> transform_batch -> load_warehouse -> quality_check -> build_analytics -> dbt_run -> dbt_test -> refresh_superset_dataset`

The current `dbt` and `Superset` tasks are placeholders in the DAG, which makes the orchestration layer ready for the next project phase.

## Data model

### Warehouse tables

`dim_coin`

- Grain: one row per coin
- Primary key: `coin_id`
- Columns: `coin_id`, `symbol`

`fact_price`

- Grain: one row per coin per ingestion timestamp
- Primary key: `(coin_id, timestamp)`
- Foreign key: `coin_id -> dim_coin.coin_id`
- Partition key for pipeline logic: `snapshot_date`
- Measures: `price`, `volume`, `market_cap`

### Analytics tables

`daily_coin_metrics`

- Grain: one row per coin per day
- Metrics: average, max, min price; average volume; average market cap; number of observations

`daily_market_summary`

- Grain: one row per day
- Metrics: total tracked coins, average price across all coins, total volume, total market cap

## Repository structure

```text
.
|-- dags/
|   |-- crypto_pipeline_dag.py
|   |-- crypto_platform_daily_dag.py
|   `-- new_pipeline_dag.py
|-- docker/
|   `-- docker-compose.yml
|-- pipeline/
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   |-- quality.py
|   `-- build_analytics.py
|-- sql/
|   `-- analytics.sql
|-- warehouse/
|   `-- schema.sql
`-- data_lake/
    |-- raw/
    `-- processed/
```

## How to run locally

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Start infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

This starts:

- Postgres
- Airflow init job
- Airflow webserver
- Airflow scheduler

### 3. Open Airflow

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

### 4. Trigger the DAG

Run the `crypto_platform_daily` DAG from the Airflow UI.

If you run the Python scripts directly from your host machine instead of Airflow, set `DATABASE_URL` to the host-mapped Postgres port:

```bash
postgresql://postgres:1234@localhost:5433/crypto_db
```

## Example analytics questions this project can answer

- Which coins had the highest average price on a given day?
- How did total crypto market volume change by day?
- Which coins were the most volatile based on daily min and max price?
- How many tracked coins were loaded on each snapshot date?

## Current strengths

- Clear batch ETL separation between extract, transform, load, and analytics steps
- Data lake layout with date-partitioned raw and processed zones
- Warehouse model with dimension and fact tables
- Airflow orchestration with parameterized `run_date` execution
- Partition-level warehouse quality checks
- Idempotent load pattern for the fact table
- Incremental analytics rebuild by `snapshot_date`

## Current limitations and next improvements

- Replace placeholder `dbt_run` and `dbt_test` tasks with a real dbt project
- Add failure alerting and richer operational monitoring
- Extend quality checks with freshness, schema drift, and business-rule validation
- Document refresh cadence, SLA expectations, and data dictionary fields more deeply
- Add cloud storage and warehouse services such as S3/GCS plus BigQuery/Redshift-style deployment
- Add Superset datasets and dashboards on top of the analytics tables
- Extend the model with SCD handling or richer dimension attributes
