select 
    coin_id,
    price,
    volume,
    market_cap,
    timestamp,
    snapshot_date
from "crypto_db"."public"."fact_price"