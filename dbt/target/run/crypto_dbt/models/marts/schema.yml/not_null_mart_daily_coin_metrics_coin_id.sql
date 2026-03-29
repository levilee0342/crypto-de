
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select coin_id
from "crypto_db"."public"."mart_daily_coin_metrics"
where coin_id is null



  
  
      
    ) dbt_internal_test