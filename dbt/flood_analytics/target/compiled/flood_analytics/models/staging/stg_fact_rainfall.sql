

select
    hour_start,
    date_key,
    time_key,
    location_key,
    rainfall_mm,
    rainfall_quality_flag,
    rainfall_present,
    rainfall_usable
from "iceberg"."dw"."fact_rainfall"