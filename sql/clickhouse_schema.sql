CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.dim_coin (
    coin_id String,
    symbol String
) 

ENGINE = MergeTree
ORDER BY coin_id;

CREATE TABLE IF NOT EXISTS crypto.fact_price (
    coin_id String,
    price Float64,
    volume Float64,
    market_cap Float64,
    timestamp DateTime,
    snapshot_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, coin_id, timestamp);

CREATE TABLE IF NOT EXISTS crypto.agg_coin_daily
(
    coin_id String,
    snapshot_date Date,
    avg_price Float64,
    max_price Float64,
    min_price Float64,
    avg_volume Float64,
    avg_market_cap Float64,
    observations UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, coin_id);

CREATE TABLE IF NOT EXISTS crypto.agg_market_daily
(
    snapshot_date Date,
    total_coins UInt64,
    avg_price_all_coins Float64,
    total_volume Float64,
    total_market_cap Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY snapshot_date;
