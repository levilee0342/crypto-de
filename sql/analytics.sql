CREATE TABLE IF NOT EXISTS daily_coin_metrics AS
SELECT
    coin_id,
    DATE(timestamp) AS snapshot_date,
    AVG(price) AS avg_price,
    MAX(price) AS max_price,
    MIN(price) AS min_price,
    AVG(volume) AS avg_volume,
    AVG(market_cap) AS avg_market_cap,
    COUNT(*) AS price_points
FROM fact_price
GROUP BY coin_id, DATE(timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_coin_metrics_pk ON daily_coin_metrics (coin_id, snapshot_date);
CREATE TABLE IF NOT EXISTS daily_market_summary AS
SELECT
    DATE(timestamp) AS snapshot_date,
    COUNT(DISTINCT coin_id) AS total_coins,
    AVG(price) AS avg_price_all_coins,
    SUM(volume) AS total_volume,
    SUM(market_cap) AS total_market_cap
FROM fact_price
GROUP BY DATE(timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_market_summary_pk ON daily_market_summary (snapshot_date);

