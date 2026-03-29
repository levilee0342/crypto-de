
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "crypto_db"."public"."stg_dim_coin"
where symbol is null



  
  
      
    ) dbt_internal_test