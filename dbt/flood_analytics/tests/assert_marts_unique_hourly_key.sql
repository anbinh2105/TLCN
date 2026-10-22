
with all_marts as (

    select
        'mart_hydrology_overview' as mart_name,
        hour_start,
        location_key
    from {{ ref('mart_hydrology_overview') }}

    union all

    select
        'mart_water_level' as mart_name,
        hour_start,
        location_key
    from {{ ref('mart_water_level') }}

    union all

    select
        'mart_rainfall' as mart_name,
        hour_start,
        location_key
    from {{ ref('mart_rainfall') }}

    union all

    select
        'mart_data_quality' as mart_name,
        hour_start,
        location_key
    from {{ ref('mart_data_quality') }}

),

duplicate_keys as (

    select
        mart_name,
        hour_start,
        location_key,
        count(*) as duplicate_count

    from all_marts

    group by
        mart_name,
        hour_start,
        location_key

    having count(*) > 1

)

select *
from duplicate_keys
