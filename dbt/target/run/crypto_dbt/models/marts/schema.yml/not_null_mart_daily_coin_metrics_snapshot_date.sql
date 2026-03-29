
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_date
from "crypto_db"."public"."mart_daily_coin_metrics"
where snapshot_date is null



  
  
      
    ) dbt_internal_test