import os 
from pathlib import Path

import pandas as pd
import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "crypto")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def load_clickhouse(run_date: str):
    processed_file = Path(f"data_lake/processed/dt={run_date}/crypto_clean.parquet")
    if not processed_file.exists():
        raise FileNotFoundError(f"Processed file not found for run_date={run_date}: {processed_file}")
    
    df = pd.read_parquet(processed_file)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    dim_df = df[["coin_id","symbol"]].drop_duplicates().reset_index(drop=True)
    fact_df = df[["coin_id", "price", "volume", "market_cap", "timestamp", "snapshot_date"]
    ].drop_duplicates(subset=["coin_id", "timestamp"]).reset_index(drop=True)

    client = get_client()

    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")

    client.command("""
        CREATE TABLE IF NOT EXISTS crypto.dim_coin
        (
            coin_id String,
            symbol String
        )
        ENGINE = MergeTree
        ORDER BY coin_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS crypto.fact_price
        (
            coin_id String,
            price Float64,
            volume Float64,
            market_cap Float64,
            timestamp DateTime,
            snapshot_date Date
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(snapshot_date)
        ORDER BY (snapshot_date, coin_id, timestamp)
    """)

    client.command(f"""
        ALTER TABLE crypto.fact_price
        DELETE WHERE snapshot_date = toDate('{run_date}')
    """)

    client.command("TRUNCATE TABLE crypto.dim_coin")

    client.insert(
        "crypto.dim_coin",
        dim_df.values.tolist(),
        column_names=["coin_id", "symbol"]
    )

    client.insert(
        "crypto.fact_price",
        fact_df.values.tolist(),
        column_names=["coin_id", 
                      "price", 
                      "volume", 
                      "market_cap", 
                      "timestamp", 
                      "snapshot_date",
                ],
    )

if __name__ == "__main__":
    load_clickhouse(run_date="2026-03-29")
            
