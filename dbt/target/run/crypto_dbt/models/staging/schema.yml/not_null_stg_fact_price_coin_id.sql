
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select coin_id
from "crypto_db"."public"."stg_fact_price"
where coin_id is null



  
  
      
    ) dbt_internal_test