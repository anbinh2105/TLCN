
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reference_alarm_level
from "iceberg"."marts"."mart_flood_risk"
where reference_alarm_level is null



  
  
      
    ) dbt_internal_test