
  create or replace view
    "iceberg"."marts_intermediate"."int_rainfall_metrics"
  security definer
  as
    

with rainfall_base as (

    select
        hour_start,
        date_key,
        time_key,
        location_key,
        rainfall_mm,
        rainfall_quality_flag,
        rainfall_present,
        rainfall_usable,

        case
            when rainfall_usable = true
                 and rainfall_mm is not null
            then rainfall_mm
            else null
        end as valid_rainfall_mm

    from "iceberg"."marts_staging"."stg_fact_rainfall"

),

rolling_metrics as (

    select
        *,

        count(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 2 preceding and current row
        ) as rainfall_valid_hours_3h,

        sum(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 2 preceding and current row
        ) as rainfall_raw_sum_3h,

        count(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 5 preceding and current row
        ) as rainfall_valid_hours_6h,

        sum(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 5 preceding and current row
        ) as rainfall_raw_sum_6h,

        count(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 11 preceding and current row
        ) as rainfall_valid_hours_12h,

        sum(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 11 preceding and current row
        ) as rainfall_raw_sum_12h,

        count(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 23 preceding and current row
        ) as rainfall_valid_hours_24h,

        sum(valid_rainfall_mm) over (
            partition by location_key
            order by hour_start
            rows between 23 preceding and current row
        ) as rainfall_raw_sum_24h

    from rainfall_base

)

select
    hour_start,
    date_key,
    time_key,
    location_key,
    rainfall_mm,
    rainfall_quality_flag,
    rainfall_present,
    rainfall_usable,

    rainfall_valid_hours_3h,
    case when rainfall_valid_hours_3h = 3
        then rainfall_raw_sum_3h
    end as rainfall_sum_3h_mm,

    rainfall_valid_hours_6h,
    case when rainfall_valid_hours_6h = 6
        then rainfall_raw_sum_6h
    end as rainfall_sum_6h_mm,

    rainfall_valid_hours_12h,
    case when rainfall_valid_hours_12h = 12
        then rainfall_raw_sum_12h
    end as rainfall_sum_12h_mm,

    rainfall_valid_hours_24h,
    case when rainfall_valid_hours_24h = 24
        then rainfall_raw_sum_24h
    end as rainfall_sum_24h_mm

from rolling_metrics
  ;
