
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    hour_start,
    location_key,
    COUNT(*) AS duplicate_count
FROM "iceberg"."marts"."mart_flood_risk"
GROUP BY hour_start, location_key
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test