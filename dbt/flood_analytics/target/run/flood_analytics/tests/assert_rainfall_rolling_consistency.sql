
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with rainfall_checks as (

    select
        hour_start,
        location_key,

        rainfall_valid_hours_3h,
        rainfall_sum_3h_mm,

        rainfall_valid_hours_6h,
        rainfall_sum_6h_mm,

        rainfall_valid_hours_12h,
        rainfall_sum_12h_mm,

        rainfall_valid_hours_24h,
        rainfall_sum_24h_mm

    from "iceberg"."marts"."mart_rainfall"

)

select *
from rainfall_checks

where
    -- 3-hour window
    rainfall_valid_hours_3h is null
    or rainfall_valid_hours_3h < 0
    or rainfall_valid_hours_3h > 3
    or (
        rainfall_valid_hours_3h = 3
        and rainfall_sum_3h_mm is null
    )
    or (
        rainfall_valid_hours_3h < 3
        and rainfall_sum_3h_mm is not null
    )

    -- 6-hour window
    or rainfall_valid_hours_6h is null
    or rainfall_valid_hours_6h < 0
    or rainfall_valid_hours_6h > 6
    or (
        rainfall_valid_hours_6h = 6
        and rainfall_sum_6h_mm is null
    )
    or (
        rainfall_valid_hours_6h < 6
        and rainfall_sum_6h_mm is not null
    )

    -- 12-hour window
    or rainfall_valid_hours_12h is null
    or rainfall_valid_hours_12h < 0
    or rainfall_valid_hours_12h > 12
    or (
        rainfall_valid_hours_12h = 12
        and rainfall_sum_12h_mm is null
    )
    or (
        rainfall_valid_hours_12h < 12
        and rainfall_sum_12h_mm is not null
    )

    -- 24-hour window
    or rainfall_valid_hours_24h is null
    or rainfall_valid_hours_24h < 0
    or rainfall_valid_hours_24h > 24
    or (
        rainfall_valid_hours_24h = 24
        and rainfall_sum_24h_mm is null
    )
    or (
        rainfall_valid_hours_24h < 24
        and rainfall_sum_24h_mm is not null
    )
  
  
      
    ) dbt_internal_test