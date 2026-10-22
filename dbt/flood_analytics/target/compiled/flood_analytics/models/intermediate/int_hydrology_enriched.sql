

with hydrology as (

    select *
    from "iceberg"."marts_staging"."stg_fact_hydrology"

),

rainfall as (

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
    from "iceberg"."marts_intermediate"."int_rainfall_metrics"

),

water_level as (

    select
        hour_start,
        location_key,
        water_level_obs_count,
        water_level_completeness,
        water_level_valid_hours_3h,
        water_level_mean_3h_m,
        water_level_valid_hours_6h,
        water_level_mean_6h_m,
        water_level_valid_hours_24h,
        water_level_max_24h_m
    from "iceberg"."marts_intermediate"."int_water_level_metrics"

),

quality as (

    select *
    from "iceberg"."marts_staging"."stg_fact_data_quality"

)

select
    h.*,

    r.rainfall_valid_hours_3h,
    r.rainfall_sum_3h_mm,
    r.rainfall_valid_hours_6h,
    r.rainfall_sum_6h_mm,
    r.rainfall_valid_hours_12h,
    r.rainfall_sum_12h_mm,
    r.rainfall_valid_hours_24h,
    r.rainfall_sum_24h_mm,

    w.water_level_obs_count,
    w.water_level_completeness,
    w.water_level_valid_hours_3h,
    w.water_level_mean_3h_m,
    w.water_level_valid_hours_6h,
    w.water_level_mean_6h_m,
    w.water_level_valid_hours_24h,
    w.water_level_max_24h_m,

    q.weather_quality_flag,
    q.weather_present,
    q.weather_usable,
    q.rainfall_quality_flag,
    q.rainfall_present,
    q.rainfall_usable,
    q.discharge_quality_flag,
    q.discharge_present,
    q.discharge_usable,
    q.water_level_quality_flag,
    q.water_level_present,
    q.water_level_usable,
    q.tide_quality_flag,
    q.tide_present,
    q.tide_usable,
    q.tide_recovery_method,
    q.all_five_present,
    q.all_five_usable

from hydrology h

left join rainfall r
    on h.hour_start = r.hour_start
   and h.location_key = r.location_key

left join water_level w
    on h.hour_start = w.hour_start
   and h.location_key = w.location_key

left join quality q
    on h.hour_start = q.hour_start
   and h.location_key = q.location_key