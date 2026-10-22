
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour_start
from "iceberg"."marts"."mart_hydrology_overview"
where hour_start is null



  
  
      
    ) dbt_internal_test