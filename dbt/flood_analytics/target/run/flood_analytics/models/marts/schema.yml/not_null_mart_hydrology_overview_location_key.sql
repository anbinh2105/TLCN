
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_key
from "iceberg"."marts"."mart_hydrology_overview"
where location_key is null



  
  
      
    ) dbt_internal_test