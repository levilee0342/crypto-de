select 
    fp.coin_id,
    fp.snapshot_date,
    avg(fp.price) as avg_price,
    max(fp.price) as max_price,
    min(fp.price) as min_price,
    avg(fp.volume) as avg_volume,
    avg(fp.market_cap) as avg_market_cap,
    count(*) as observations
from {{ ref('stg_fact_price') }} as fp
group by 
    fp.coin_id,
    fp.snapshot_date