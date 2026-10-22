SELECT
    hour_start,
    location_key,
    COUNT(*) AS duplicate_count
FROM "iceberg"."marts"."mart_flood_risk"
GROUP BY hour_start, location_key
HAVING COUNT(*) > 1