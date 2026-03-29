select 
    coin_id,
    price,
    volume,
    market_cap,
    timestamp,
    snapshot_date
from {{ source('crypto', 'fact_price') }}
