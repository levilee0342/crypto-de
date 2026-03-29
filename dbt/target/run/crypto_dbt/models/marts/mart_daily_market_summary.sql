
  create view "crypto_db"."public"."mart_daily_market_summary__dbt_tmp"
    
    
  as (
    select 
    snapshot_date,
    count(distinct coin_id) as total_coins,
    avg(price) as avg_price_all_coins,
    sum(volume) as total_volume,
    sum(market_cap) as total_market_cap
from "crypto_db"."public"."stg_fact_price"
group by snapshot_date
  );