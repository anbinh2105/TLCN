"""Step 11.4 - Build Iceberg snowflake dimensions from Gold Batch (idempotent).
Run from project root: python -m warehouse.dimensions.load_dimensions
Requires the exact 11.3 DDL to have been applied already.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from transformation.silver.common import create_spark_session, stop_spark

SOURCE = 'lakehouse.gold.hourly_hydrology'
NS = 'lakehouse.dw'
EXPECTED_COUNT = 73656
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports' / 'warehouse' / 'dimensions'

TABLES = ['dim_year', 'dim_month', 'dim_date', 'dim_time', 'dim_region',
          'dim_location', 'dim_station_type', 'dim_station']


def verify_gold(df):
    missing = set(['hour_start', 'location_id', 'water_level_station_code',
                   'discharge_station', 'tide_reference_location', 'tide_publisher']) - set(df.columns)
    if missing:
        raise RuntimeError(f'Gold missing columns: {sorted(missing)}')
    n = df.count()
    if n != EXPECTED_COUNT:
        raise RuntimeError(f'Gold baseline mismatch: {n} != {EXPECTED_COUNT}')
    if df.filter(F.col('hour_start').isNull() | F.col('location_id').isNull()).limit(1).count():
        raise RuntimeError('Gold has null business keys')
    if df.groupBy('hour_start', 'location_id').count().filter('count > 1').limit(1).count():
        raise RuntimeError('Gold has duplicate business keys')
    return n


def dimension_frames(spark, df):
    dates = df.select(F.to_date('hour_start').alias('full_date')).distinct()
    dim_year = dates.select(F.year('full_date').alias('year_key')).distinct().withColumn('calendar_year', F.col('year_key'))
    dim_month = (dates.select(F.year('full_date').alias('year_key'), F.month('full_date').alias('month_number'))
                 .distinct().withColumn('month_key', F.col('year_key') * 100 + F.col('month_number'))
                 .withColumn('quarter_number', F.quarter(F.make_date('year_key', 'month_number', F.lit(1))))
                 .select('month_key', 'year_key', 'month_number', 'quarter_number'))
    dim_date = (dates.withColumn('date_key', F.date_format('full_date', 'yyyyMMdd').cast('int'))
                .withColumn('month_key', F.year('full_date') * 100 + F.month('full_date'))
                .withColumn('day_of_month', F.dayofmonth('full_date'))
                .withColumn('day_of_week', F.dayofweek('full_date'))
                .select('date_key', 'full_date', 'month_key', 'day_of_month', 'day_of_week'))
    dim_time = (spark.range(24).select(F.col('id').cast('int').alias('time_key'))
                .withColumn('hour_of_day', F.col('time_key'))
                .withColumn('time_label', F.format_string('%02d:00', F.col('time_key')))
                .withColumn('day_part', F.when(F.col('time_key') < 6, 'NIGHT')
                            .when(F.col('time_key') < 12, 'MORNING')
                            .when(F.col('time_key') < 18, 'AFTERNOON').otherwise('EVENING')))
    # No authoritative administrative region metadata in Gold: keep empty.
    empty_region_schema = StructType([StructField('region_key', StringType()), StructField('region_name', StringType())])
    dim_region = spark.createDataFrame([], empty_region_schema)
    dim_location = (df.select('location_id').distinct()
                    .withColumn('location_key', F.col('location_id'))
                    .withColumn('region_key', F.lit(None).cast('string'))
                    .withColumn('location_name', F.lit(None).cast('string'))
                    .select('location_key', 'location_id', 'region_key', 'location_name'))
    roles = [('WATER_LEVEL', 'Water-level observation station'),
             ('DISCHARGE', 'River discharge source'),
             ('TIDE_REFERENCE', 'Tide reference location')]
    dim_station_type = spark.createDataFrame(roles, ['station_role', 'station_type_description'])\
        .withColumn('station_type_key', F.col('station_role'))\
        .select('station_type_key', 'station_role', 'station_type_description')
    wl = (df.select(F.col('water_level_station_code').alias('station_source_id')).where('station_source_id IS NOT NULL')
          .distinct().withColumn('station_type_key', F.lit('WATER_LEVEL'))
          .withColumn('station_name', F.lit(None).cast('string'))
          .withColumn('publisher', F.lit(None).cast('string')))
    discharge = (df.select(F.col('discharge_station').alias('station_source_id')).where('station_source_id IS NOT NULL')
                 .distinct().withColumn('station_type_key', F.lit('DISCHARGE'))
                 .withColumn('station_name', F.col('station_source_id'))
                 .withColumn('publisher', F.lit(None).cast('string')))
    tide = (df.select(F.col('tide_reference_location').alias('station_source_id'),
                      F.col('tide_publisher').alias('publisher'))
            .where('station_source_id IS NOT NULL').distinct()
            .withColumn('station_type_key', F.lit('TIDE_REFERENCE'))
            .withColumn('station_name', F.col('station_source_id')))
    station_rows = wl.unionByName(discharge).unionByName(tide)
    # Stable role-specific key. Publisher is an attribute, not part of station identity.
    # Reject contradictory publishers for the same role/source key rather than arbitrarily choosing one.
    station_rows = station_rows.withColumn('station_key', F.concat_ws('::', 'station_type_key', 'station_source_id'))
    if station_rows.groupBy('station_key').count().filter('count > 1').limit(1).count():
        raise RuntimeError('Conflicting station metadata for a role/source key; investigate before loading')
    dim_station = station_rows.select('station_key', 'station_type_key', 'station_source_id', 'station_name', 'publisher')
    return dict(dim_year=dim_year, dim_month=dim_month, dim_date=dim_date,
                dim_time=dim_time, dim_region=dim_region, dim_location=dim_location,
                dim_station_type=dim_station_type, dim_station=dim_station)


def check_unique(df, name, key):
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise RuntimeError(f'{name}: NULL primary key {key}')
    if df.groupBy(key).count().filter('count > 1').limit(1).count():
        raise RuntimeError(f'{name}: duplicate primary key {key}')


def ensure_fk(child, parent, child_key, parent_key, label):
    missing = (child.where(F.col(child_key).isNotNull()).select(child_key).distinct()
               .join(parent.select(F.col(parent_key).alias(child_key)).distinct(), child_key, 'left_anti'))
    if missing.limit(1).count():
        raise RuntimeError(f'Broken dimension FK: {label}')


def main():
    spark = None
    report = {'stage': '11.4', 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
              'source': SOURCE, 'mode': 'insert_only_if_empty_else_verify', 'tables': [], 'overall_status': 'FAIL'}
    try:
        spark = create_spark_session(app_name='cantho-warehouse-dimensions', master='local[2]')
        if not spark.catalog.tableExists(SOURCE):
            raise RuntimeError(f'Source missing: {SOURCE}')
        gold = spark.table(SOURCE)
        report['gold_record_count'] = verify_gold(gold)
        frames = dimension_frames(spark, gold)
        keys = {'dim_year':'year_key', 'dim_month':'month_key', 'dim_date':'date_key',
                'dim_time':'time_key', 'dim_region':'region_key', 'dim_location':'location_key',
                'dim_station_type':'station_type_key', 'dim_station':'station_key'}
        for name, frame in frames.items():
            check_unique(frame, name, keys[name])
        ensure_fk(frames['dim_month'], frames['dim_year'], 'year_key', 'year_key', 'month -> year')
        ensure_fk(frames['dim_date'], frames['dim_month'], 'month_key', 'month_key', 'date -> month')
        ensure_fk(frames['dim_location'], frames['dim_region'], 'region_key', 'region_key', 'location -> region')
        ensure_fk(frames['dim_station'], frames['dim_station_type'], 'station_type_key', 'station_type_key', 'station -> station_type')
        missing_tables = [name for name in TABLES if not spark.catalog.tableExists(f'{NS}.{name}')]
        if missing_tables:
            raise RuntimeError(f'Missing Iceberg tables: {missing_tables}. Apply 11.3 DDL first.')
        # Preflight every table schema and existing contents BEFORE making any writes.
        plan = []
        for name in TABLES:
            table = f'{NS}.{name}'
            target = spark.table(table)
            frame = frames[name]
            expected_cols = target.columns
            if set(frame.columns) != set(expected_cols):
                raise RuntimeError(f'{name}: schema column mismatch: target={expected_cols}, source={frame.columns}')
            frame = frame.select(*expected_cols)
            expected_n = frame.count()
            actual_n = target.count()
            if actual_n == 0:
                action = 'EMPTY' if expected_n == 0 else 'INSERT'
            else:
                if actual_n != expected_n or frame.exceptAll(target).limit(1).count() or target.exceptAll(frame).limit(1).count():
                    raise RuntimeError(f'{name}: existing data differs; refusing to overwrite')
                action = 'VERIFY_NOOP'
            plan.append((name, frame, expected_n, action))
        print('=== PREFLIGHT PLAN ===')
        for name, _, n, action in plan:
            print(f'{name}: {n} rows -> {action}')
        for name, frame, n, action in plan:
            if action == 'INSERT':
                frame.writeTo(f'{NS}.{name}').append()
            actual = spark.table(f'{NS}.{name}')
            if actual.count() != n or frame.exceptAll(actual).limit(1).count() or actual.exceptAll(frame).limit(1).count():
                raise RuntimeError(f'{name}: post-write verification failed')
            report['tables'].append({'table': f'{NS}.{name}', 'record_count': n,
                                     'action': action, 'status': 'PASS'})
            print(f'[PASS] {name}: {n} rows ({action})')
        report['overall_status'] = 'PASS'
    except Exception as exc:
        report['error'] = str(exc)
        print(f'[FAIL] {exc}')
        raise
    finally:
        OUT.mkdir(parents=True, exist_ok=True)
        out = OUT / ('dimensions_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '.json')
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f'REPORT: {out}')
        print(f'OVERALL: {report["overall_status"]}')
        if spark is not None:
            stop_spark(spark)


if __name__ == '__main__':
    main()
