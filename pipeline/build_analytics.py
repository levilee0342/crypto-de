import os

from sqlalchemy import create_engine, text

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost:5432/crypto_db"
)

engine = create_engine(DB_URL)

def build_analytics():
    with engine.begin() as conn:
        conn.execute(text("""DROP TABLE IF EXISTS daily_coin_metrics"""))
        conn.execute(text("""DROP TABLE IF EXISTS daily_market_summary"""))

        conn.execute(text("""
            CREATE TABLE daily_coin_metrics AS
            SELECT 
                coin_id,
                DATE(timestamp) AS snapshot_date,
                AVG(price) AS avg_price,
                MAX(price) AS max_price,
                MIN(price) AS min_price,
                AVG(volume) AS avg_volume,
                AVG(market_cap) AS avg_market_cap,
                COUNT(*) AS observations
            FROM fact_price
            GROUP BY coin_id, DATE(timestamp)
        """))

        conn.execute(text("""CREATE UNIQUE INDEX idx_daily_coin_metrics ON daily_coin_metrics (coin_id, snapshot_date)"""))

        conn.execute(text("""
            CREATE TABLE daily_market_summary AS  
            SELECT 
                DATE(timestamp) AS snapshot_date,
                COUNT(DISTINCT coin_id) AS total_coins,
                AVG(price) AS avg_price_all_coins,
                SUM(volume) AS total_volume,
                SUM(market_cap) AS total_market_cap
            FROM fact_price
            GROUP BY DATE(timestamp)
        """))

        conn.execute(text("""CREATE UNIQUE INDEX idx_daily_market_summary ON daily_market_summary (snapshot_date)"""))

if __name__ == "__main__":
    build_analytics()