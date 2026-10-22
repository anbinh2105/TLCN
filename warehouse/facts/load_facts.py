"""Step 11.5 - Safe initial load of four Iceberg facts from Gold Batch.
Run from TLCN root: python -m warehouse.facts.load_facts
No overwrite or automatic merge. Existing matching data -> VERIFY_NOOP.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from transformation.silver.common import create_spark_session, stop_spark

SOURCE = 'lakehouse.gold.hourly_hydrology'
NS = 'lakehouse.dw'
EXPECTED_COUNT = 73656
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports' / 'warehouse' / 'facts'
FACTS = ('fact_hydrology', 'fact_water_level', 'fact_rainfall', 'fact_data_quality')


def has_rows(df):
    return df.limit(1).count() > 0


def assert_unique_not_null(df, columns, label):
    if has_rows(df.filter(F.col(columns[0]).isNull() | F.col(columns[1]).isNull())):
        raise RuntimeError(f'{label}: NULL business key')
    if has_rows(df.groupBy(*columns).count().filter(F.col('count') > 1)):
        raise RuntimeError(f'{label}: duplicate business key')


def ensure_fk(df, column, dim, dim_key):
    if not dim.columns or dim_key not in dim.columns:
        raise RuntimeError(f'Dimension missing key {dim_key} for {column}')
    missing = (df.filter(F.col(column).isNotNull()).select(column).distinct()
               .join(dim.select(F.col(dim_key).alias(column)).distinct(), column, 'left_anti'))
    if has_rows(missing):
        sample = [r[0] for r in missing.limit(5).collect()]
        raise RuntimeError(f'Unmatched FK {column} -> {dim_key}: {sample}')


def build_frames(gold):
    base = (gold.withColumn('date_key', F.date_format('hour_start', 'yyyyMMdd').cast('int'))
            .withColumn('time_key', F.hour('hour_start').cast('int'))
            .withColumn('location_key', F.col('location_id'))
            .withColumn('water_level_station_key',
                        F.when(F.col('water_level_station_code').isNotNull(),
                               F.concat(F.lit('WATER_LEVEL::'), F.col('water_level_station_code'))))
            .withColumn('discharge_station_key',
                        F.when(F.col('discharge_station').isNotNull(),
                               F.concat(F.lit('DISCHARGE::'), F.col('discharge_station'))))
            .withColumn('tide_reference_station_key',
                        F.when(F.col('tide_reference_location').isNotNull(),
                               F.concat(F.lit('TIDE_REFERENCE::'), F.col('tide_reference_location')))))
    columns = {
        'fact_hydrology': [
            'hour_start', 'date_key', 'time_key', 'location_key',
            'water_level_station_key', 'discharge_station_key', 'tide_reference_station_key',
            'temperature_c', 'humidity_pct', 'pressure_hpa', 'wind_speed', 'wind_direction',
            'rainfall_mm', 'discharge_m3s', 'discharge_alignment_method',
            'discharge_source_temporal_grain', 'discharge_is_hourly_observation',
            'water_level_mean', 'water_level_min', 'water_level_max', 'water_level_last',
            'tide_height_m', 'tide_is_recovered', 'present_source_count',
            'usable_source_count', 'gold_processed_at'],
        'fact_water_level': [
            'hour_start', 'date_key', 'time_key', 'location_key', 'water_level_station_key',
            'water_level_mean', 'water_level_min', 'water_level_max', 'water_level_last',
            'water_level_last_timestamp', 'water_level_obs_count', 'water_level_total_rows',
            'water_level_on_grid_count', 'water_level_off_grid_count',
            'water_level_non_observed_count', 'water_level_expected_count',
            'water_level_completeness', 'water_level_quality_flag',
            'water_level_present', 'water_level_usable'],
        'fact_rainfall': [
            'hour_start', 'date_key', 'time_key', 'location_key',
            'rainfall_mm', 'rainfall_quality_flag', 'rainfall_present', 'rainfall_usable'],
        'fact_data_quality': [
            'hour_start', 'date_key', 'time_key', 'location_key',
            'weather_quality_flag', 'weather_present', 'weather_usable',
            'rainfall_quality_flag', 'rainfall_present', 'rainfall_usable',
            'discharge_quality_flag', 'discharge_present', 'discharge_usable',
            'water_level_quality_flag', 'water_level_present', 'water_level_usable',
            'tide_quality_flag', 'tide_present', 'tide_usable', 'tide_recovery_method',
            'present_source_count', 'usable_source_count', 'all_five_present', 'all_five_usable']}
    for name, cols in columns.items():
        missing = sorted(set(cols) - set(base.columns))
        if missing:
            raise RuntimeError(f'{name}: Gold missing columns: {missing}')
    return base, {name: base.select(*cols) for name, cols in columns.items()}


def identical(left, right):
    # Check both directions: avoids mistaking an incomplete target for an identical one.
    return not has_rows(left.exceptAll(right)) and not has_rows(right.exceptAll(left))


def main():
    spark = None
    report = {'stage': '11.5', 'mode': 'insert_only_if_empty_else_verify',
              'source': SOURCE, 'expected_count': EXPECTED_COUNT,
              'generated_at_utc': datetime.now(timezone.utc).isoformat(),
              'tables': [], 'overall_status': 'FAIL'}
    try:
        spark = create_spark_session(app_name='cantho-warehouse-facts', master='local[2]')
        spark.conf.set('spark.sql.session.timeZone', 'Asia/Ho_Chi_Minh')
        if not spark.catalog.tableExists(SOURCE):
            raise RuntimeError(f'Missing Gold source: {SOURCE}')
        gold = spark.table(SOURCE)
        source_count = gold.count()
        report['source_count'] = source_count
        if source_count != EXPECTED_COUNT:
            raise RuntimeError(f'Gold baseline changed: {source_count} != {EXPECTED_COUNT}')
        assert_unique_not_null(gold, ('hour_start', 'location_id'), 'Gold')
        base, frames = build_frames(gold)

        dimensions = {
            'date_key': ('dim_date', 'date_key'),
            'time_key': ('dim_time', 'time_key'),
            'location_key': ('dim_location', 'location_key'),
            'water_level_station_key': ('dim_station', 'station_key'),
            'discharge_station_key': ('dim_station', 'station_key'),
            'tide_reference_station_key': ('dim_station', 'station_key')}
        dim_cache = {}
        for _, (name, _) in dimensions.items():
            if name not in dim_cache:
                full = f'{NS}.{name}'
                if not spark.catalog.tableExists(full):
                    raise RuntimeError(f'Missing Dimension: {full}')
                dim_cache[name] = spark.table(full)
        for key, (dim_name, dim_key) in dimensions.items():
            ensure_fk(base, key, dim_cache[dim_name], dim_key)

        plan = []
        for name in FACTS:
            full = f'{NS}.{name}'
            if not spark.catalog.tableExists(full):
                raise RuntimeError(f'Missing Fact table: {full}. Apply 11.3 DDL first.')
            target = spark.table(full)
            frame = frames[name]
            if set(target.columns) != set(frame.columns):
                raise RuntimeError(f'{name}: schema mismatch. target={target.columns}, frame={frame.columns}')
            # Cast only when types are compatible; do not silently change meaning.
            target_types = {f.name: f.dataType for f in target.schema.fields}
            for f in frame.schema.fields:
                if f.dataType != target_types[f.name]:
                    raise RuntimeError(f'{name}: type mismatch {f.name}: {f.dataType} != {target_types[f.name]}')
            frame = frame.select(*target.columns)
            assert_unique_not_null(frame, ('hour_start', 'location_key'), name)
            n = frame.count()
            if n != EXPECTED_COUNT:
                raise RuntimeError(f'{name}: unexpected row count {n}')
            existing = target.count()
            if existing == 0:
                action = 'INSERT'
            elif existing == n and identical(frame, target):
                action = 'VERIFY_NOOP'
            else:
                raise RuntimeError(f'{name}: target already contains different data ({existing} rows); no overwrite')
            plan.append((name, frame, n, action))
        print('=== PREFLIGHT PLAN ===')
        for name, _, n, action in plan:
            print(f'{name}: {n} rows -> {action}')

        for name, frame, n, action in plan:
            full = f'{NS}.{name}'
            if action == 'INSERT':
                frame.writeTo(full).append()
            target = spark.table(full)
            if target.count() != n or not identical(frame, target):
                raise RuntimeError(f'{name}: post-write verification failed')
            report['tables'].append({'table': full, 'record_count': n,
                                     'action': action, 'status': 'PASS'})
            print(f'[PASS] {name}: {n} rows ({action})')
        if spark.table(SOURCE).count() != EXPECTED_COUNT:
            raise RuntimeError('Gold baseline changed during load')
        report['overall_status'] = 'PASS'
    except Exception as exc:
        report['error'] = str(exc)
        print(f'[FAIL] {exc}')
        raise
    finally:
        OUT.mkdir(parents=True, exist_ok=True)
        out = OUT / ('facts_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '.json')
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'REPORT: {out}')
        print(f'OVERALL: {report["overall_status"]}')
        if spark is not None:
            stop_spark(spark)


if __name__ == '__main__':
    main()
