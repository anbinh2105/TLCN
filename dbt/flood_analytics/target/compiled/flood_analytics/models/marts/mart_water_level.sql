

select
    hour_start,
    date_key,
    time_key,
    location_key,
    water_level_station_key,

    -- Hourly water level statistics
    water_level_mean,
    water_level_min,
    water_level_max,
    water_level_last,
    water_level_last_timestamp,

    -- Observation metadata
    water_level_obs_count,
    water_level_total_rows,
    water_level_on_grid_count,
    water_level_off_grid_count,
    water_level_non_observed_count,
    water_level_expected_count,
    water_level_completeness,

    -- Rolling statistics
    water_level_valid_hours_3h,
    water_level_mean_3h_m,

    water_level_valid_hours_6h,
    water_level_mean_6h_m,

    water_level_valid_hours_24h,
    water_level_max_24h_m,

    -- Data quality
    water_level_quality_flag,
    water_level_present,
    water_level_usable

from "iceberg"."marts_intermediate"."int_water_level_metrics"