
  
    

    create table "iceberg"."marts"."mart_flood_risk"
      
      
    as (
      

WITH water_level AS (
    SELECT
        hour_start,
        date_key,
        time_key,
        location_key,
        water_level_station_key,
        water_level_mean,
        water_level_max,
        water_level_obs_count,
        water_level_present,
        water_level_usable
    FROM "iceberg"."dw"."fact_water_level"
),

thresholds AS (
    SELECT
        station_key,
        station_code,
        alarm_level_1_m,
        alarm_level_2_m,
        alarm_level_3_m,
        legal_document,
        effective_from,
        vertical_datum,
        datum_status
    FROM "iceberg"."dw"."dim_flood_threshold"
),

joined AS (
    SELECT
        w.*,
        t.station_code,
        t.alarm_level_1_m,
        t.alarm_level_2_m,
        t.alarm_level_3_m,
        t.legal_document,
        t.effective_from,
        t.vertical_datum,
        t.datum_status,

        CASE
            WHEN t.station_key IS NULL
                THEN 'THRESHOLD_NOT_FOUND'
            WHEN t.datum_status = 'VERIFIED'
                THEN 'DATUM_VERIFIED'
            ELSE 'REFERENCE_ONLY'
        END AS threshold_comparison_status,

        CASE
            WHEN t.effective_from IS NULL THEN NULL
            WHEN CAST(w.hour_start AS DATE) >= t.effective_from
                THEN TRUE
            ELSE FALSE
        END AS within_legal_effective_period

    FROM water_level w
    LEFT JOIN thresholds t
        ON w.water_level_station_key = t.station_key
),

classified AS (
    SELECT
        *,

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
        END AS reference_alarm_level

    FROM joined
)

SELECT
    hour_start,
    date_key,
    time_key,
    location_key,
    water_level_station_key,
    station_code,

    water_level_mean,
    water_level_max,
    water_level_obs_count,
    water_level_present,
    water_level_usable,

    alarm_level_1_m,
    alarm_level_2_m,
    alarm_level_3_m,

    legal_document,
    effective_from,
    vertical_datum,
    datum_status,
    threshold_comparison_status,
    within_legal_effective_period,

    reference_alarm_level,

    CASE
        WHEN datum_status = 'VERIFIED'
             AND within_legal_effective_period = TRUE
             AND reference_alarm_level <> 'UNKNOWN'
            THEN reference_alarm_level
        ELSE CAST(NULL AS VARCHAR)
    END AS official_alarm_level

FROM classified
    );

  