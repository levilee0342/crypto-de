
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp
from "crypto_db"."public"."stg_fact_price"
where timestamp is null



  
  
      
    ) dbt_internal_test