
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_date
from "crypto_db"."public"."stg_fact_price"
where snapshot_date is null



  
  
      
    ) dbt_internal_test