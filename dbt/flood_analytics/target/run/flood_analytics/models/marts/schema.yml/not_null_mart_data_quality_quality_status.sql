
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quality_status
from "iceberg"."marts"."mart_data_quality"
where quality_status is null



  
  
      
    ) dbt_internal_test