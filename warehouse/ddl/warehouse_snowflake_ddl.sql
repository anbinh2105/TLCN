-- Step 11.3: Proposed Iceberg DDL (schema only, no data writes)
-- Execute via Spark SQL with the existing lakehouse Hive Iceberg catalog.
-- Logical foreign keys are documented, not enforced by Iceberg.
CREATE NAMESPACE IF NOT EXISTS lakehouse.dw;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_year (
  year_key INT,
  calendar_year INT
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_month (
  month_key INT,
  year_key INT,
  month_number INT,
  quarter_number INT
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_date (
  date_key INT,
  full_date DATE,
  month_key INT,
  day_of_month INT,
  day_of_week INT
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_time (
  time_key INT,
  hour_of_day INT,
  time_label STRING,
  day_part STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_region (
  region_key STRING,
  region_name STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_location (
  location_key STRING,
  location_id STRING,
  region_key STRING,
  location_name STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_station_type (
  station_type_key STRING,
  station_role STRING,
  station_type_description STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.dim_station (
  station_key STRING,
  station_type_key STRING,
  station_source_id STRING,
  station_name STRING,
  publisher STRING
) USING iceberg;

-- Grain for each fact: hour_start x location_key.
-- Station role-specific keys are nullable; never assume all stations are identical.
CREATE TABLE IF NOT EXISTS lakehouse.dw.fact_hydrology (
  hour_start TIMESTAMP,
  date_key INT,
  time_key INT,
  location_key STRING,
  water_level_station_key STRING,
  discharge_station_key STRING,
  tide_reference_station_key STRING,
  temperature_c DOUBLE,
  humidity_pct DOUBLE,
  pressure_hpa DOUBLE,
  wind_speed DOUBLE,
  wind_direction DOUBLE,
  rainfall_mm DOUBLE,
  discharge_m3s DOUBLE,
  discharge_alignment_method STRING,
  discharge_source_temporal_grain STRING,
  discharge_is_hourly_observation BOOLEAN,
  water_level_mean DOUBLE,
  water_level_min DOUBLE,
  water_level_max DOUBLE,
  water_level_last DOUBLE,
  tide_height_m DOUBLE,
  tide_is_recovered BOOLEAN,
  present_source_count INT,
  usable_source_count INT,
  gold_processed_at TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.fact_water_level (
  hour_start TIMESTAMP,
  date_key INT,
  time_key INT,
  location_key STRING,
  water_level_station_key STRING,
  water_level_mean DOUBLE,
  water_level_min DOUBLE,
  water_level_max DOUBLE,
  water_level_last DOUBLE,
  water_level_last_timestamp TIMESTAMP,
  water_level_obs_count BIGINT,
  water_level_total_rows BIGINT,
  water_level_on_grid_count BIGINT,
  water_level_off_grid_count BIGINT,
  water_level_non_observed_count BIGINT,
  water_level_expected_count INT,
  water_level_completeness DOUBLE,
  water_level_quality_flag STRING,
  water_level_present BOOLEAN,
  water_level_usable BOOLEAN
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.fact_rainfall (
  hour_start TIMESTAMP,
  date_key INT,
  time_key INT,
  location_key STRING,
  rainfall_mm DOUBLE,
  rainfall_quality_flag STRING,
  rainfall_present BOOLEAN,
  rainfall_usable BOOLEAN
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.dw.fact_data_quality (
  hour_start TIMESTAMP,
  date_key INT,
  time_key INT,
  location_key STRING,
  weather_quality_flag STRING,
  weather_present BOOLEAN,
  weather_usable BOOLEAN,
  rainfall_quality_flag STRING,
  rainfall_present BOOLEAN,
  rainfall_usable BOOLEAN,
  discharge_quality_flag STRING,
  discharge_present BOOLEAN,
  discharge_usable BOOLEAN,
  water_level_quality_flag STRING,
  water_level_present BOOLEAN,
  water_level_usable BOOLEAN,
  tide_quality_flag STRING,
  tide_present BOOLEAN,
  tide_usable BOOLEAN,
  tide_recovery_method STRING,
  present_source_count INT,
  usable_source_count INT,
  all_five_present BOOLEAN,
  all_five_usable BOOLEAN
) USING iceberg;
