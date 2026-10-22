
    
    

with all_values as (

    select
        quality_status as value_field,
        count(*) as n_records

    from "iceberg"."marts"."mart_data_quality"
    group by quality_status

)

select *
from all_values
where value_field not in (
    'COMPLETE','PARTIAL','NO_USABLE_DATA'
)


