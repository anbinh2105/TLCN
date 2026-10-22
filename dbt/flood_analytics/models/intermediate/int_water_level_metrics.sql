
{{ config(materialized='view') }}

with water_level_base as (

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
        water_level_usable,

        case
            when water_level_usable = true
                 and water_level_mean is not null
                 and water_level_max is not null
                 and water_level_obs_count > 0
            then water_level_mean
        end as valid_water_level_mean,

        case
            when water_level_usable = true
                 and water_level_mean is not null
                 and water_level_max is not null
                 and water_level_obs_count > 0
            then water_level_max
        end as valid_water_level_max

    from {{ ref('stg_fact_water_level') }}

),

rolling_metrics as (

    select
        *,

        count(valid_water_level_mean) over (
            partition by location_key
            order by hour_start
            rows between 2 preceding and current row
        ) as water_level_valid_hours_3h,

        avg(valid_water_level_mean) over (
            partition by location_key
            order by hour_start
            rows between 2 preceding and current row
        ) as raw_mean_3h,

        count(valid_water_level_mean) over (
            partition by location_key
            order by hour_start
            rows between 5 preceding and current row
        ) as water_level_valid_hours_6h,

        avg(valid_water_level_mean) over (
            partition by location_key
            order by hour_start
            rows between 5 preceding and current row
        ) as raw_mean_6h,

        count(valid_water_level_max) over (
            partition by location_key
            order by hour_start
            rows between 23 preceding and current row
        ) as water_level_valid_hours_24h,

        max(valid_water_level_max) over (
            partition by location_key
            order by hour_start
            rows between 23 preceding and current row
        ) as raw_max_24h

    from water_level_base

)

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
    water_level_usable,

    water_level_valid_hours_3h,
    case
        when water_level_valid_hours_3h = 3
        then raw_mean_3h
    end as water_level_mean_3h_m,

    water_level_valid_hours_6h,
    case
        when water_level_valid_hours_6h = 6
        then raw_mean_6h
    end as water_level_mean_6h_m,

    water_level_valid_hours_24h,
    case
        when water_level_valid_hours_24h = 24
        then raw_max_24h
    end as water_level_max_24h_m

from rolling_metrics
