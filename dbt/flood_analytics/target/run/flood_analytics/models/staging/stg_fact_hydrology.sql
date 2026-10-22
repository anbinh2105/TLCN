
  create or replace view
    "iceberg"."marts_staging"."stg_fact_hydrology"
  security definer
  as
    

select
    hour_start,
    date_key,
    time_key,
    location_key,
    water_level_station_key,
    discharge_station_key,
    tide_reference_station_key,
    temperature_c,
    humidity_pct,
    pressure_hpa,
    wind_speed,
    wind_direction,
    rainfall_mm,
    discharge_m3s,
    discharge_alignment_method,
    discharge_source_temporal_grain,
    discharge_is_hourly_observation,
    water_level_mean,
    water_level_min,
    water_level_max,
    water_level_last,
    tide_height_m,
    tide_is_recovered,
    present_source_count,
    usable_source_count,
    gold_processed_at
from "iceberg"."dw"."fact_hydrology"
  ;
