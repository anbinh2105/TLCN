
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        reference_alarm_level as value_field,
        count(*) as n_records

    from "iceberg"."marts"."mart_flood_risk"
    group by reference_alarm_level

)

select *
from all_values
where value_field not in (
    'BELOW_LEVEL_1','LEVEL_1','LEVEL_2','LEVEL_3_OR_ABOVE','UNKNOWN'
)



  
  
      
    ) dbt_internal_test