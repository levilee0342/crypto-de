import os
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

def clickhouse_quality_check(run_date: str):
    client = get_client()
    
    fact_count_result = client.query(f"""
        SELECT COUNT()
        FROM crypto.fact_price
        WHERE snapshot_date = toDate('{run_date}')
    """)
    fact_count = fact_count_result.result_rows[0][0]

    negative_price_result = client.query(f"""
        SELECT COUNT()
        FROM crypto.fact_price
        WHERE snapshot_date = toDate('{run_date}')
        AND price < 0
    """)
    negative_price_count = negative_price_result.result_rows[0][0]

    
    if fact_count == 0:
        raise ValueError(f"ClickHouse quality check failed: no rows for run_date={run_date}")

    if negative_price_count > 0:
        raise ValueError(
            f"ClickHouse quality check failed: {negative_price_count} negative price rows for run_date={run_date}"
        )
    
if __name__ == "__main__":
    clickhouse_quality_check("2026-03-29")