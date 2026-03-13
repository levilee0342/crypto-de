import os 
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost:5432/crypto_db"
)

engine = create_engine(DB_URL)


def load():
    processed_base = Path("data_lake/processed")
    processed_file = sorted(processed_base.glob("dt=*/crypto_clean.parquet"))
    if not processed_file:
        raise FileNotFoundError("No processed file found")

    last_processed = processed_file[-1]
    df = pd.read_parquet(last_processed)
  
    dim_df = df[["coin_id","symbol"]].drop_duplicates().reset_index(drop=True)
    fact_df = df.drop(columns=["symbol"]).drop_duplicates(subset=["coin_id", "timestamp"]).reset_index(drop=True)

    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS dim_coin (
            coin_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL
        )"""))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_price (
                coin_id TEXT NOT NULL,
                price DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                market_cap DOUBLE PRECISION,
                timestamp TIMESTAMP NOT NULL,
                PRIMARY KEY (coin_id, timestamp),
                CONSTRAINT fk_fact_price_coin
                    FOREIGN KEY (coin_id) REFERENCES dim_coin (coin_id)
            )
        """))

        for row in dim_df.itertuples(index=False):
            conn.execute(text("""
                INSERT INTO dim_coin (coin_id, symbol)
                VALUES (:coin_id, :symbol)
                ON CONFLICT (coin_id) 
                DO UPDATE SET symbol = EXCLUDED.symbol
            """), 
            {"coin_id": row.coin_id, "symbol": row.symbol})


        for row in fact_df.itertuples(index=False):
            conn.execute(text("""
                INSERT INTO fact_price (coin_id, price, volume, market_cap, timestamp)
                VALUES (:coin_id, :price, :volume, :market_cap, :timestamp)
                ON CONFLICT (coin_id, timestamp) 
                DO NOTHING
            """), 
            {
                "coin_id": row.coin_id,
                "price": row.price,
                "volume": row.volume,
                "market_cap": row.market_cap,
                "timestamp": row.timestamp
            })

if __name__ == "__main__":
    load()