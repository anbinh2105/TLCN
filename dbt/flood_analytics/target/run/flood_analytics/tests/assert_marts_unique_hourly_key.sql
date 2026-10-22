
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with all_marts as (

    select
        'mart_hydrology_overview' as mart_name,
        hour_start,
        location_key
    from "iceberg"."marts"."mart_hydrology_overview"

    union all

    select
        'mart_water_level' as mart_name,
        hour_start,
        location_key
    from "iceberg"."marts"."mart_water_level"

    union all

    select
        'mart_rainfall' as mart_name,
        hour_start,
        location_key
    from "iceberg"."marts"."mart_rainfall"

    union all

    select
        'mart_data_quality' as mart_name,
        hour_start,
        location_key
    from "iceberg"."marts"."mart_data_quality"

),

duplicate_keys as (

    select
        mart_name,
        hour_start,
        location_key,
        count(*) as duplicate_count

    from all_marts

    group by
        mart_name,
        hour_start,
        location_key

    having count(*) > 1

)

select *
from duplicate_keys
  
  
      
    ) dbt_internal_test