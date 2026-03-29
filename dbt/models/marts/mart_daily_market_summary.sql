select 
    snapshot_date,
    count(distinct coin_id) as total_coins,
    avg(price) as avg_price_all_coins,
    sum(volume) as total_volume,
    sum(market_cap) as total_market_cap
from {{ ref('stg_fact_price') }}
group by snapshot_date