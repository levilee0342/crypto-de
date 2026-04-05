import os
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost:5432/crypto_db"
)

engine = create_engine(DB_URL)

def quality_check(run_date: str):
    logger.info("Starting quality checks for run_date=%s", run_date)

    with engine.begin() as conn:
        fact_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_price WHERE snapshot_date = :run_date"), {"run_date": run_date}).scalar()

        null_coin_id_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_price WHERE coin_id IS NULL AND snapshot_date = :run_date"), {"run_date": run_date}).scalar()
        
        null_timestamp_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_price WHERE timestamp IS NULL AND snapshot_date = :run_date"), {"run_date": run_date}
        ).scalar()

        duplicate_count = conn.execute(
            text("""
                 SELECT COUNT(*) FROM (
                    SELECT coin_id, timestamp, COUNT(*) AS cnt
                    FROM fact_price
                    WHERE snapshot_date = :run_date
                    GROUP BY coin_id, timestamp
                    HAVING COUNT(*) > 1
                 ) AS duplicates_rows
            """), {"run_date": run_date}
        ).scalar()

        orphan_fact_count = conn.execute(
            text("""
                    SELECT COUNT(*) 
                    FROM fact_price f
                    LEFT JOIN dim_coin d
                        ON f.coin_id = d.coin_id
                    WHERE d.coin_id IS NULL AND f.snapshot_date = :run_date
            """), {"run_date": run_date}
        ).scalar()

        freshness_count = conn.execute(
            text("""
                SELECT COUNT(*) 
                FROM fact_price
                WHERE snapshot_date = :run_date
            """), {"run_date": run_date}
        ).scalar()

        negative_price_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM fact_price
            WHERE snapshot_date = :run_date
            AND price < 0
        """), {"run_date": run_date}).scalar()

        negative_volume_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM fact_price
            WHERE snapshot_date = :run_date
            AND volume < 0
        """), {"run_date": run_date}).scalar()

        negative_market_cap_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM fact_price
            WHERE snapshot_date = :run_date
            AND market_cap < 0
        """), {"run_date": run_date}).scalar()

        distinct_coin_count = conn.execute(text("""
            SELECT COUNT(DISTINCT coin_id)
            FROM fact_price
            WHERE snapshot_date = :run_date
        """), {"run_date": run_date}).scalar()

        logger.info("run_date=%s fact_count=%s", run_date, fact_count)
        logger.info("run_date=%s null_coin_id_count=%s", run_date, null_coin_id_count)
        logger.info("run_date=%s null_timestamp_count=%s", run_date, null_timestamp_count)
        logger.info("run_date=%s duplicate_count=%s", run_date, duplicate_count)
        logger.info("run_date=%s orphan_fact_count=%s", run_date, orphan_fact_count)

        if fact_count == 0:
            raise ValueError(f"Quality check failed: no fact_price rows found for run_date={run_date}")

        if null_coin_id_count > 0:
            raise ValueError(
                f"Quality check failed: {null_coin_id_count} rows with null coin_id for run_date={run_date}"
            )

        if null_timestamp_count > 0:
            raise ValueError(
                f"Quality check failed: {null_timestamp_count} rows with null timestamp for run_date={run_date}"
            )

        if duplicate_count > 0:
            raise ValueError(
                f"Quality check failed: {duplicate_count} duplicate (coin_id, timestamp) groups for run_date={run_date}"
            )

        if orphan_fact_count > 0:
            raise ValueError(
                f"Quality check failed: {orphan_fact_count} orphan fact rows for run_date={run_date}"
            )
        
        if freshness_count == 0:
            raise ValueError(f"Freshness check failed: no data found for run_date={run_date}")

        if negative_price_count > 0:
            raise ValueError(f"Range check failed: {negative_price_count} negative price rows for run_date={run_date}")

        if negative_volume_count > 0:
            raise ValueError(f"Range check failed: {negative_volume_count} negative volume rows for run_date={run_date}")

        if negative_market_cap_count > 0:
            raise ValueError(f"Range check failed: {negative_market_cap_count} negative market_cap rows for run_date={run_date}")

        if distinct_coin_count < 10:
            raise ValueError(
                f"Minimum coin count check failed: only {distinct_coin_count} coins found for run_date={run_date}"
            )
        
    logger.info("Quality checks passed for run_date=%s", run_date)