

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
    rainfall_sum_3h_mm,
    rainfall_sum_6h_mm,
    rainfall_sum_12h_mm,
    rainfall_sum_24h_mm,

    water_level_mean,
    water_level_min,
    water_level_max,
    water_level_last,
    water_level_mean_3h_m,
    water_level_mean_6h_m,
    water_level_max_24h_m,

    tide_height_m,
    tide_is_recovered,

    discharge_m3s,
    discharge_alignment_method,
    discharge_source_temporal_grain,
    discharge_is_hourly_observation,

    present_source_count,
    usable_source_count,
    all_five_present,
    all_five_usable,

    weather_quality_flag,
    rainfall_quality_flag,
    water_level_quality_flag,
    discharge_quality_flag,
    tide_quality_flag

from "iceberg"."marts_intermediate"."int_hydrology_enriched"