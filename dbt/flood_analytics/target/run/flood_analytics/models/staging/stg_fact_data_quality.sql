
  create or replace view
    "iceberg"."marts_staging"."stg_fact_data_quality"
  security definer
  as
    

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
from "iceberg"."dw"."fact_data_quality"
  ;
