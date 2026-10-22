with mart_locations as (

    select distinct
        location_key
    from "iceberg"."marts"."mart_hydrology_overview"

    union

    select distinct
        location_key
    from "iceberg"."marts"."mart_water_level"

    union

    select distinct
        location_key
    from "iceberg"."marts"."mart_rainfall"

    union

    select distinct
        location_key
    from "iceberg"."marts"."mart_data_quality"

),

dim_locations as (

    select
        location_key
    from "iceberg"."dw"."dim_location"

)

select
    m.location_key
from mart_locations m

left join dim_locations d
    on m.location_key = d.location_key

where d.location_key is null