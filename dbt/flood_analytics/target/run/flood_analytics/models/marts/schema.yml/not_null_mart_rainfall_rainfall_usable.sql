
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rainfall_usable
from "iceberg"."marts"."mart_rainfall"
where rainfall_usable is null



  
  
      
    ) dbt_internal_test