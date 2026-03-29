
  create view "crypto_db"."public"."stg_fact_price__dbt_tmp"
    
    
  as (
    select 
    coin_id,
    price,
    volume,
    market_cap,
    timestamp,
    snapshot_date
from "crypto_db"."public"."fact_price"
  );