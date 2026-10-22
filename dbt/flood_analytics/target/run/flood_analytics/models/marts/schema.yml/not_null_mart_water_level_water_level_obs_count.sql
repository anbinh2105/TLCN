
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select water_level_obs_count
from "iceberg"."marts"."mart_water_level"
where water_level_obs_count is null



  
  
      
    ) dbt_internal_test