import os
import logging
from sqlalchemy import create_engine, text


logger = logging.getLogger(__name__)

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost:5432/crypto_db"
)

engine = create_engine(DB_URL)

def analytics_quality_check(run_date: str):
    logger.info("Starting analytics quality checks for run_date=%s", run_date)

    with engine.begin() as conn:
        mart_coin_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM mart_daily_coin_metrics
            WHERE snapshot_date = :run_date
        """), {"run_date": run_date}).scalar()

        mart_market_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM mart_daily_market_summary
            WHERE snapshot_date = :run_date
        """), {"run_date": run_date}).scalar()

        logger.info("run_date=%s mart_daily_coin_metrics rows=%s", run_date, mart_coin_count)
        logger.info("run_date=%s mart_daily_market_summary rows=%s", run_date, mart_market_count)

        if mart_coin_count == 0:
            raise ValueError(
                f"Analytics quality check failed: mart_daily_coin_metrics has no rows for run_date={run_date}"
            )

        if mart_market_count == 0:
            raise ValueError(
                f"Analytics quality check failed: mart_daily_market_summary has no rows for run_date={run_date}"
            )

    logger.info("Analytics quality checks passed for run_date=%s", run_date)
