
{{ config(materialized='table') }}

select
    hour_start,
    date_key,
    time_key,
    location_key,

    -- Hourly rainfall
    rainfall_mm,

    -- Accumulated rainfall
    rainfall_valid_hours_3h,
    rainfall_sum_3h_mm,

    rainfall_valid_hours_6h,
    rainfall_sum_6h_mm,

    rainfall_valid_hours_12h,
    rainfall_sum_12h_mm,

    rainfall_valid_hours_24h,
    rainfall_sum_24h_mm,

    -- Data quality
    rainfall_quality_flag,
    rainfall_present,
    rainfall_usable

from {{ ref('int_rainfall_metrics') }}
