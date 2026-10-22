
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        threshold_comparison_status as value_field,
        count(*) as n_records

    from "iceberg"."marts"."mart_flood_risk"
    group by threshold_comparison_status

)

select *
from all_values
where value_field not in (
    'THRESHOLD_NOT_FOUND','DATUM_VERIFIED','REFERENCE_ONLY'
)



  
  
      
    ) dbt_internal_test