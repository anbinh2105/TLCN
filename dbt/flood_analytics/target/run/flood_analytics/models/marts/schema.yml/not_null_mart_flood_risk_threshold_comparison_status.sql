
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select threshold_comparison_status
from "iceberg"."marts"."mart_flood_risk"
where threshold_comparison_status is null



  
  
      
    ) dbt_internal_test