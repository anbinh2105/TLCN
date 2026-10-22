
SELECT *
FROM {{ ref('mart_flood_risk') }}
WHERE
    (
        alarm_level_1_m IS NOT NULL
        AND alarm_level_2_m IS NOT NULL
        AND alarm_level_3_m IS NOT NULL
        AND NOT (
            alarm_level_1_m < alarm_level_2_m
            AND alarm_level_2_m < alarm_level_3_m
        )
    )
    OR (
        official_alarm_level IS NOT NULL
        AND (
            datum_status IS DISTINCT FROM 'VERIFIED'
            OR within_legal_effective_period IS DISTINCT FROM TRUE
            OR reference_alarm_level = 'UNKNOWN'
        )
    )
    OR (
        datum_status = 'UNVERIFIED'
        AND threshold_comparison_status <> 'REFERENCE_ONLY'
    )
