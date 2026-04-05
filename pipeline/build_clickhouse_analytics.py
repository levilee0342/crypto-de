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


def build_clickhouse_analytics(run_date: str):
    client = get_client()

    client.command(f"""
        ALTER TABLE crypto.agg_coin_daily
        DELETE WHERE snapshot_date = toDate('{run_date}')
    """)
    
    client.command(f"""
        ALTER TABLE crypto.agg_market_daily
        DELETE WHERE snapshot_date = toDate('{run_date}')
    """)

    client.command(f"""
        INSERT INTO crypto.agg_coin_daily 
        SELECT 
            coin_id, 
            snapshot_date,
            avg(price) AS avg_price,
            max(price) AS max_price,
            min(price) AS min_price,
            avg(volume) AS avg_volume,
            avg(market_cap) AS avg_market_cap,
            count() as observations
            FROM crypto.fact_price
            WHERE snapshot_date = toDate('{run_date}')
            GROUP BY coin_id, snapshot_date
    """)

    client.command(f"""
        INSERT INTO crypto.agg_market_daily
        SELECT 
            snapshot_date,
            count(distinct coin_id) as total_coins,
            avg(price) as avg_price_all_coins,
            sum(volume) as total_volume,
            sum(market_cap) as total_market_cap
        FROM crypto.fact_price
        WHERE snapshot_date = toDate('{run_date}')
        GROUP BY snapshot_date
    """)

if __name__ == "__main__":
    build_clickhouse_analytics("2026-03-29")