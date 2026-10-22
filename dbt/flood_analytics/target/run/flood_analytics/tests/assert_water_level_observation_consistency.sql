
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    hour_start,
    location_key,
    water_level_usable,
    water_level_obs_count,
    water_level_mean

from "iceberg"."marts"."mart_water_level"

where
    (
        water_level_usable = true
        and (
            water_level_obs_count is null
            or water_level_obs_count <= 0
            or water_level_mean is null
        )
    )
    or
    (
        water_level_obs_count < 0
    )
  
  
      
    ) dbt_internal_test