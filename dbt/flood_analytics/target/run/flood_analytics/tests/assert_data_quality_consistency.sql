
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with quality_checks as (

    select
        *,

        (
            case when weather_present then 1 else 0 end +
            case when rainfall_present then 1 else 0 end +
            case when discharge_present then 1 else 0 end +
            case when water_level_present then 1 else 0 end +
            case when tide_present then 1 else 0 end
        ) as calculated_present_count,

        (
            case when weather_usable then 1 else 0 end +
            case when rainfall_usable then 1 else 0 end +
            case when discharge_usable then 1 else 0 end +
            case when water_level_usable then 1 else 0 end +
            case when tide_usable then 1 else 0 end
        ) as calculated_usable_count

    from "iceberg"."marts"."mart_data_quality"

)

select
    hour_start,
    location_key,
    present_source_count,
    usable_source_count,
    calculated_present_count,
    calculated_usable_count,
    quality_status

from quality_checks

where
    -- Source counts
    present_source_count is distinct from calculated_present_count
    or usable_source_count is distinct from calculated_usable_count

    -- Derived counts
    or missing_source_count is distinct from
        (5 - present_source_count)

    or unusable_source_count is distinct from
        (present_source_count - usable_source_count)

    -- Ratios
    or source_presence_ratio is distinct from
        (cast(present_source_count as double) / 5.0)

    or source_usability_ratio is distinct from
        (cast(usable_source_count as double) / 5.0)

    -- Overall flags
    or all_five_present is distinct from
        (present_source_count = 5)

    or all_five_usable is distinct from
        (usable_source_count = 5)

    -- Status classification
    or quality_status is distinct from
        (
            case
                when usable_source_count = 5 then 'COMPLETE'
                when usable_source_count = 0 then 'NO_USABLE_DATA'
                else 'PARTIAL'
            end
        )

    -- Usable data must also be present
    or (weather_usable and not weather_present)
    or (rainfall_usable and not rainfall_present)
    or (discharge_usable and not discharge_present)
    or (water_level_usable and not water_level_present)
    or (tide_usable and not tide_present)
  
  
      
    ) dbt_internal_test