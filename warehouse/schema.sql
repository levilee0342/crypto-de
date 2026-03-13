CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_price (
    coin_id TEXT NOT NULL,
    price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (coin_id, timestamp),
    CONSTRAINT fk_fact_price_coin
        FOREIGN KEY (coin_id) REFERENCES dim_coin (coin_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_price_timestamp
    ON fact_price (timestamp);

CREATE INDEX IF NOT EXISTS idx_fact_price_coin_id
    ON fact_price (coin_id);
