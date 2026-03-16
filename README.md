# Crypto Data Engineering Pipeline

An end-to-end batch data engineering project that ingests cryptocurrency market data from the CoinGecko API, stores raw and processed datasets in Parquet, loads a Postgres warehouse, and builds daily analytics tables with Apache Airflow orchestration.

## What this project demonstrates

- API ingestion with Python and `requests`
- Raw and processed data lake layers in Parquet
- Batch ETL orchestration with Apache Airflow
- Dimensional warehouse loading into Postgres
- Idempotent fact loading with conflict handling
- Daily aggregate analytics for downstream reporting
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

`pipeline/extract.py` calls the CoinGecko `/coins/markets` endpoint, adds a UTC ingestion timestamp, and writes raw API output to:

`data_lake/raw/dt=YYYY-MM-DD/crypto.parquet`

### 2. Transform

`pipeline/transform.py` selects and standardizes the core columns used by the warehouse:

- `coin_id`
- `symbol`
- `price`
- `volume`
- `market_cap`
- `timestamp`

The transformed dataset is written to:

`data_lake/processed/dt=YYYY-MM-DD/crypto_clean.parquet`

### 3. Load

`pipeline/load.py` loads data into a small dimensional warehouse in Postgres:

- `dim_coin` stores one row per coin
- `fact_price` stores one row per `coin_id` and `timestamp`

Loading behavior:

- Dimension rows are upserted with `ON CONFLICT DO UPDATE`
- Fact rows are deduplicated by `(coin_id, timestamp)`
- Repeat runs for the same snapshot do not create duplicate fact rows

### 4. Build analytics

`pipeline/build_analytics.py` builds two daily aggregate tables:

- `daily_coin_metrics`
- `daily_market_summary`

These tables support simple reporting and SQL-based analysis on top of the warehouse.

### 5. Orchestration

`dags/crypto_pipeline_dag.py` defines a daily Airflow DAG:

`extract -> transform -> load -> build_analytics`

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
|   `-- crypto_pipeline_dag.py
|-- docker/
|   `-- docker-compose.yml
|-- pipeline/
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
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

Run the `crypto_pipeline` DAG from the Airflow UI.

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
- Airflow orchestration for repeatable scheduled runs
- Idempotent load pattern for the fact table

## Current limitations and next improvements

This project is a strong Fresher Data Engineer portfolio base, but the next upgrades to make it look more production-oriented are:

- Add data quality checks for nulls, duplicates, schema drift, and freshness
- Add logging, task-level monitoring, and failure alerting
- Document refresh cadence, SLA expectations, and data dictionary fields more deeply
- Introduce incremental analytics builds instead of dropping and rebuilding aggregate tables
- Add cloud storage and warehouse services such as S3/GCS plus BigQuery/Redshift-style deployment
- Add dbt models and tests for a more analytics-engineering workflow
- Extend the model with SCD handling or richer dimension attributes

## Why this project is relevant for Data Engineering roles

This repository is positioned as a DE portfolio project rather than a generic backend app. It shows the ability to:

- ingest data from an external API
- persist raw and curated datasets
- model warehouse tables with clear grain and keys
- orchestrate repeatable jobs
- produce analytics-ready tables for downstream consumers

That combination aligns well with Fresher or early Junior Data Engineer expectations, especially when paired with strong SQL and a clear explanation of warehouse design decisions.
