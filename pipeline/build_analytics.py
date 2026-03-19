import os

import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost:5432/crypto_db"
)

engine = create_engine(DB_URL)

def build_analytics(run_date: str):
    logger.info("Building analytics for run_date=%s", run_date)
    
    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_coin_metrics (
                coin_id TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                avg_price DOUBLE PRECISION,
                max_price DOUBLE PRECISION,
                min_price DOUBLE PRECISION,
                avg_volume DOUBLE PRECISION,
                avg_market_cap DOUBLE PRECISION,
                observations INTEGER,
                PRIMARY KEY (coin_id, snapshot_date)
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_market_summary (
                snapshot_date DATE PRIMARY KEY,
                total_coins INTEGER,
                avg_price_all_coins DOUBLE PRECISION,
                total_volume DOUBLE PRECISION,
                total_market_cap DOUBLE PRECISION
            )
        """))

        conn.execute(text("""
            DELETE FROM daily_coin_metrics WHERE snapshot_date = :run_date
        """), {"run_date": run_date})

        conn.execute(text("""
            DELETE FROM daily_market_summary WHERE snapshot_date = :run_date
        """), {"run_date": run_date})

        conn.execute(text("""
            INSERT INTO daily_coin_metrics (coin_id, snapshot_date, avg_price, max_price, min_price, avg_volume, avg_market_cap, observations)
            SELECT
                coin_id,
                snapshot_date,
                AVG(price) AS avg_price,
                MAX(price) AS max_price,
                MIN(price) AS min_price,
                AVG(volume) AS avg_volume,
                AVG(market_cap) AS avg_market_cap,
                COUNT(*) AS observations
            FROM fact_price
            WHERE snapshot_date = :run_date
            GROUP BY coin_id, snapshot_date
        """), {"run_date": run_date})
                          

        conn.execute(text("""
            INSERT INTO daily_market_summary (
                snapshot_date,
                total_coins,
                avg_price_all_coins,
                total_volume,
                total_market_cap
            )
            SELECT
                snapshot_date,
                COUNT(DISTINCT coin_id) AS total_coins,
                AVG(price) AS avg_price_all_coins,
                SUM(volume) AS total_volume,
                SUM(market_cap) AS total_market_cap
            FROM fact_price
            WHERE snapshot_date = :run_date
            GROUP BY snapshot_date
        """), {"run_date": run_date})

    logger.info("Finished analytics build for run_date=%s", run_date)

if __name__ == "__main__":
    build_analytics("2026-03-17")