
  create or replace view
    "iceberg"."marts_staging"."stg_fact_water_level"
  security definer
  as
    

select
    hour_start,
    date_key,
    time_key,
    location_key,
    water_level_station_key,
    water_level_mean,
    water_level_min,
    water_level_max,
    water_level_last,
    water_level_last_timestamp,
    water_level_obs_count,
    water_level_total_rows,
    water_level_on_grid_count,
    water_level_off_grid_count,
    water_level_non_observed_count,
    water_level_expected_count,
    water_level_completeness,
    water_level_quality_flag,
    water_level_present,
    water_level_usable
from "iceberg"."dw"."fact_water_level"
  ;
