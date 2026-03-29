
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    snapshot_date as unique_field,
    count(*) as n_records

from "crypto_db"."public"."mart_daily_market_summary"
where snapshot_date is not null
group by snapshot_date
having count(*) > 1



  
  
      
    ) dbt_internal_test