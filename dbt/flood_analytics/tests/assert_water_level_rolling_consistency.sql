
with water_level_checks as (

    select
        hour_start,
        location_key,

        water_level_valid_hours_3h,
        water_level_mean_3h_m,

        water_level_valid_hours_6h,
        water_level_mean_6h_m,

        water_level_valid_hours_24h,
        water_level_max_24h_m

    from {{ ref('mart_water_level') }}

)

select *
from water_level_checks

where
    -- 3-hour window
    water_level_valid_hours_3h is null
    or water_level_valid_hours_3h < 0
    or water_level_valid_hours_3h > 3
    or (
        water_level_valid_hours_3h = 3
        and water_level_mean_3h_m is null
    )
    or (
        water_level_valid_hours_3h < 3
        and water_level_mean_3h_m is not null
    )

    -- 6-hour window
    or water_level_valid_hours_6h is null
    or water_level_valid_hours_6h < 0
    or water_level_valid_hours_6h > 6
    or (
        water_level_valid_hours_6h = 6
        and water_level_mean_6h_m is null
    )
    or (
        water_level_valid_hours_6h < 6
        and water_level_mean_6h_m is not null
    )

    -- 24-hour window
    or water_level_valid_hours_24h is null
    or water_level_valid_hours_24h < 0
    or water_level_valid_hours_24h > 24
    or (
        water_level_valid_hours_24h = 24
        and water_level_max_24h_m is null
    )
    or (
        water_level_valid_hours_24h < 24
        and water_level_max_24h_m is not null
    )
