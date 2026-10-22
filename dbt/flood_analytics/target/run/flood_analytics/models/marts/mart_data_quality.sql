
  
    

    create table "iceberg"."marts"."mart_data_quality"
      
      
    as (
      

select
    hour_start,
    date_key,
    time_key,
    location_key,

    -- Weather
    weather_quality_flag,
    weather_present,
    weather_usable,

    -- Rainfall
    rainfall_quality_flag,
    rainfall_present,
    rainfall_usable,

    -- River discharge
    discharge_quality_flag,
    discharge_present,
    discharge_usable,

    -- Water level
    water_level_quality_flag,
    water_level_present,
    water_level_usable,

    -- Tide
    tide_quality_flag,
    tide_present,
    tide_usable,
    tide_recovery_method,

    -- Source coverage
    present_source_count,
    usable_source_count,
    missing_source_count,
    unusable_source_count,

    source_presence_ratio,
    source_usability_ratio,

    all_five_present,
    all_five_usable,

    -- Overall quality classification
    quality_status

from "iceberg"."marts_intermediate"."int_data_quality_summary"
    );

  