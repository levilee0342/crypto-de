
  create view "crypto_db"."public"."stg_dim_coin__dbt_tmp"
    
    
  as (
    select 
    coin_id,
    symbol
from "crypto_db"."public"."dim_coin"
  );