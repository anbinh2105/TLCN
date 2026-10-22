
with all_keys as (

    select
        hour_start,
        location_key,
        'hydrology' as source_name
    from {{ ref('mart_hydrology_overview') }}

    union all

    select
        hour_start,
        location_key,
        'water_level' as source_name
    from {{ ref('mart_water_level') }}

    union all

    select
        hour_start,
        location_key,
        'rainfall' as source_name
    from {{ ref('mart_rainfall') }}

    union all

    select
        hour_start,
        location_key,
        'data_quality' as source_name
    from {{ ref('mart_data_quality') }}

),

key_coverage as (

    select
        hour_start,
        location_key,
        count(distinct source_name) as mart_count

    from all_keys

    group by
        hour_start,
        location_key

)

select *
from key_coverage
where mart_count <> 4
