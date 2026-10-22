

with quality_base as (

    select
        hour_start,
        date_key,
        time_key,
        location_key,

        weather_quality_flag,
        weather_present,
        weather_usable,

        rainfall_quality_flag,
        rainfall_present,
        rainfall_usable,

        discharge_quality_flag,
        discharge_present,
        discharge_usable,

        water_level_quality_flag,
        water_level_present,
        water_level_usable,

        tide_quality_flag,
        tide_present,
        tide_usable,
        tide_recovery_method,

        present_source_count,
        usable_source_count,
        all_five_present,
        all_five_usable

    from "iceberg"."marts_staging"."stg_fact_data_quality"

)

select
    *,

    5 - present_source_count
        as missing_source_count,

    present_source_count - usable_source_count
        as unusable_source_count,

    cast(present_source_count as double) / 5.0
        as source_presence_ratio,

    cast(usable_source_count as double) / 5.0
        as source_usability_ratio,

    case
        when usable_source_count = 5
            then 'COMPLETE'
        when usable_source_count = 0
            then 'NO_USABLE_DATA'
        else 'PARTIAL'
    end as quality_status

from quality_base