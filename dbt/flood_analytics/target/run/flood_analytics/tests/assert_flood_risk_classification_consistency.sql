
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  WITH expected AS (
    SELECT
        hour_start,
        location_key,
        reference_alarm_level,

        CASE
            WHEN NOT COALESCE(water_level_usable, FALSE)
              OR water_level_max IS NULL
              OR water_level_obs_count IS NULL
              OR water_level_obs_count <= 0
              OR alarm_level_1_m IS NULL
              OR alarm_level_2_m IS NULL
              OR alarm_level_3_m IS NULL
                THEN 'UNKNOWN'
            WHEN water_level_max >= alarm_level_3_m
                THEN 'LEVEL_3_OR_ABOVE'
            WHEN water_level_max >= alarm_level_2_m
                THEN 'LEVEL_2'
            WHEN water_level_max >= alarm_level_1_m
                THEN 'LEVEL_1'
            ELSE 'BELOW_LEVEL_1'
        END AS expected_level

    FROM "iceberg"."marts"."mart_flood_risk"
)

SELECT *
FROM expected
WHERE reference_alarm_level <> expected_level
   OR reference_alarm_level IS NULL
  
  
      
    ) dbt_internal_test