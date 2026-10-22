# Data Dictionary — lakehouse.dw

Generated from actual Iceberg table schemas. Nullable describes schema allowance, not observed completeness.

## lakehouse.dw.dim_year

- Row count: 9

| Column | Spark type | Nullable |
|---|---|---|
| `year_key` | `int` | True |
| `calendar_year` | `int` | True |

## lakehouse.dw.dim_month

- Row count: 102

| Column | Spark type | Nullable |
|---|---|---|
| `month_key` | `int` | True |
| `year_key` | `int` | True |
| `month_number` | `int` | True |
| `quarter_number` | `int` | True |

## lakehouse.dw.dim_date

- Row count: 3,069

| Column | Spark type | Nullable |
|---|---|---|
| `date_key` | `int` | True |
| `full_date` | `date` | True |
| `month_key` | `int` | True |
| `day_of_month` | `int` | True |
| `day_of_week` | `int` | True |

## lakehouse.dw.dim_time

- Row count: 24

| Column | Spark type | Nullable |
|---|---|---|
| `time_key` | `int` | True |
| `hour_of_day` | `int` | True |
| `time_label` | `string` | True |
| `day_part` | `string` | True |

## lakehouse.dw.dim_region

- Row count: 0

| Column | Spark type | Nullable |
|---|---|---|
| `region_key` | `string` | True |
| `region_name` | `string` | True |

## lakehouse.dw.dim_location

- Row count: 1

| Column | Spark type | Nullable |
|---|---|---|
| `location_key` | `string` | True |
| `location_id` | `string` | True |
| `region_key` | `string` | True |
| `location_name` | `string` | True |

## lakehouse.dw.dim_station_type

- Row count: 3

| Column | Spark type | Nullable |
|---|---|---|
| `station_type_key` | `string` | True |
| `station_role` | `string` | True |
| `station_type_description` | `string` | True |

## lakehouse.dw.dim_station

- Row count: 3

| Column | Spark type | Nullable |
|---|---|---|
| `station_key` | `string` | True |
| `station_type_key` | `string` | True |
| `station_source_id` | `string` | True |
| `station_name` | `string` | True |
| `publisher` | `string` | True |

## lakehouse.dw.fact_hydrology

- Row count: 73,656

| Column | Spark type | Nullable |
|---|---|---|
| `hour_start` | `timestamp` | True |
| `date_key` | `int` | True |
| `time_key` | `int` | True |
| `location_key` | `string` | True |
| `water_level_station_key` | `string` | True |
| `discharge_station_key` | `string` | True |
| `tide_reference_station_key` | `string` | True |
| `temperature_c` | `double` | True |
| `humidity_pct` | `double` | True |
| `pressure_hpa` | `double` | True |
| `wind_speed` | `double` | True |
| `wind_direction` | `double` | True |
| `rainfall_mm` | `double` | True |
| `discharge_m3s` | `double` | True |
| `discharge_alignment_method` | `string` | True |
| `discharge_source_temporal_grain` | `string` | True |
| `discharge_is_hourly_observation` | `boolean` | True |
| `water_level_mean` | `double` | True |
| `water_level_min` | `double` | True |
| `water_level_max` | `double` | True |
| `water_level_last` | `double` | True |
| `tide_height_m` | `double` | True |
| `tide_is_recovered` | `boolean` | True |
| `present_source_count` | `int` | True |
| `usable_source_count` | `int` | True |
| `gold_processed_at` | `timestamp` | True |

## lakehouse.dw.fact_water_level

- Row count: 73,656

| Column | Spark type | Nullable |
|---|---|---|
| `hour_start` | `timestamp` | True |
| `date_key` | `int` | True |
| `time_key` | `int` | True |
| `location_key` | `string` | True |
| `water_level_station_key` | `string` | True |
| `water_level_mean` | `double` | True |
| `water_level_min` | `double` | True |
| `water_level_max` | `double` | True |
| `water_level_last` | `double` | True |
| `water_level_last_timestamp` | `timestamp` | True |
| `water_level_obs_count` | `bigint` | True |
| `water_level_total_rows` | `bigint` | True |
| `water_level_on_grid_count` | `bigint` | True |
| `water_level_off_grid_count` | `bigint` | True |
| `water_level_non_observed_count` | `bigint` | True |
| `water_level_expected_count` | `int` | True |
| `water_level_completeness` | `double` | True |
| `water_level_quality_flag` | `string` | True |
| `water_level_present` | `boolean` | True |
| `water_level_usable` | `boolean` | True |

## lakehouse.dw.fact_rainfall

- Row count: 73,656

| Column | Spark type | Nullable |
|---|---|---|
| `hour_start` | `timestamp` | True |
| `date_key` | `int` | True |
| `time_key` | `int` | True |
| `location_key` | `string` | True |
| `rainfall_mm` | `double` | True |
| `rainfall_quality_flag` | `string` | True |
| `rainfall_present` | `boolean` | True |
| `rainfall_usable` | `boolean` | True |

## lakehouse.dw.fact_data_quality

- Row count: 73,656

| Column | Spark type | Nullable |
|---|---|---|
| `hour_start` | `timestamp` | True |
| `date_key` | `int` | True |
| `time_key` | `int` | True |
| `location_key` | `string` | True |
| `weather_quality_flag` | `string` | True |
| `weather_present` | `boolean` | True |
| `weather_usable` | `boolean` | True |
| `rainfall_quality_flag` | `string` | True |
| `rainfall_present` | `boolean` | True |
| `rainfall_usable` | `boolean` | True |
| `discharge_quality_flag` | `string` | True |
| `discharge_present` | `boolean` | True |
| `discharge_usable` | `boolean` | True |
| `water_level_quality_flag` | `string` | True |
| `water_level_present` | `boolean` | True |
| `water_level_usable` | `boolean` | True |
| `tide_quality_flag` | `string` | True |
| `tide_present` | `boolean` | True |
| `tide_usable` | `boolean` | True |
| `tide_recovery_method` | `string` | True |
| `present_source_count` | `int` | True |
| `usable_source_count` | `int` | True |
| `all_five_present` | `boolean` | True |
| `all_five_usable` | `boolean` | True |
