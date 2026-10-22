"""S8.4 read-only validation of Gold Batch isolation from streaming replay.
Run from TLCN project root: python -m streaming.demo.validate_batch_protection
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from transformation.silver.common import create_spark_session

BATCH = 'lakehouse.gold.hourly_hydrology'
STREAM = {
    'silver': ('lakehouse.silver.water_level_stream', 10),
    'gold_hourly': ('lakehouse.gold.water_level_hourly_stream', 3),
    'gold_integrated': ('lakehouse.gold.hourly_hydrology_stream', 3),
}
BASELINE_COUNT = 73656
BASELINE_SNAPSHOT = '5232994262495386058'
KEYS = ('hour_start', 'location_id')


def snapshot_id(spark, table):
    # Iceberg metadata table; current snapshot is obtained from the table's history.
    rows = spark.sql(f'SELECT snapshot_id FROM {table}.history WHERE is_current_ancestor = true ORDER BY made_current_at DESC LIMIT 1').collect()
    if not rows:
        raise RuntimeError(f'No current Iceberg snapshot found: {table}')
    return str(rows[0]['snapshot_id'])


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--expected-count', type=int, default=BASELINE_COUNT)
    parser.add_argument('--expected-snapshot', default=BASELINE_SNAPSHOT)
    args = parser.parse_args()

    checks = []
    report = {
        'stage': 'S8.4', 'mode': 'read_only',
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'baseline': {'count': args.expected_count, 'snapshot_id': args.expected_snapshot},
        'checks': checks,
    }

    def check(name, passed, observed, expected=None):
        item = {'name': name, 'status': 'PASS' if passed else 'FAIL', 'observed': observed}
        if expected is not None:
            item['expected'] = expected
        checks.append(item)
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: {observed}", flush=True)

    spark = None
    try:
        spark = create_spark_session('S8.4-Batch-Gold-Protection-ReadOnly')
        spark.conf.set('spark.sql.session.timeZone', 'Asia/Ho_Chi_Minh')
        exists = spark.catalog.tableExists(BATCH)
        check('batch_table_exists', exists, exists, True)
        if not exists:
            raise RuntimeError('Batch table does not exist')

        df = spark.table(BATCH)
        count = df.count()
        check('batch_record_count', count == args.expected_count, count, args.expected_count)

        # Kiểm tra các cột khóa nghiệp vụ có tồn tại
        missing_keys = sorted(set(KEYS) - set(df.columns))

        check(
            'batch_key_columns',
            len(missing_keys) == 0,
            list(KEYS),
            list(KEYS) if not missing_keys else {
                'required': list(KEYS),
                'missing': missing_keys
            }
        )

        if missing_keys:
            raise RuntimeError(
                f'Missing batch key columns: {missing_keys}'
            )

        null_condition = F.col(KEYS[0]).isNull() | F.col(KEYS[1]).isNull()
        null_keys = df.filter(null_condition).count()
        check('batch_null_keys', null_keys == 0, null_keys, 0)

        duplicate_groups = (df.groupBy(*KEYS).count().filter(F.col('count') > 1).count())
        check('batch_duplicate_key_groups', duplicate_groups == 0, duplicate_groups, 0)

        current_snapshot = snapshot_id(spark, BATCH)
        check('batch_snapshot_unchanged', current_snapshot == args.expected_snapshot,
              current_snapshot, args.expected_snapshot)

        # The replay schema uses explicit simulation and origin markers.
        # Only evaluate these if the Batch schema actually contains them.
        for col_name in ('simulation', 'mixed_simulation'):
            if col_name in df.columns:
                flagged = df.filter(F.coalesce(F.col(col_name).cast('boolean'), F.lit(False))).count()
                check(f'batch_{col_name}_true_rows', flagged == 0, flagged, 0)
            else:
                check(f'batch_{col_name}_column_absent', True, 'not present (not a replay marker)', 'not present')

        if 'water_level_origin' in df.columns:
            flagged = df.filter(F.col('water_level_origin') == 'STREAM_REPLAY').count()
            check('batch_stream_replay_origin_rows', flagged == 0, flagged, 0)
        else:
            check('batch_stream_replay_origin_column_absent', True,
                  'not present (not a replay marker)', 'not present')

        for label, (table, expected) in STREAM.items():
            present = spark.catalog.tableExists(table)
            check(f'{label}_table_exists', present, present, True)
            if present:
                observed = spark.table(table).count()
                check(f'{label}_record_count', observed == expected, observed, expected)

        report['status'] = 'PASS' if all(c['status'] == 'PASS' for c in checks) else 'FAIL'
    except Exception as exc:
        report['status'] = 'FAIL'
        report['error'] = f'{type(exc).__name__}: {exc}'
        print('ERROR:', report['error'], flush=True)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                report['spark_stop_warning'] = str(exc)
        out_dir = Path('reports/streaming_demo')
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"batch_protection_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        out_path = out_dir / filename
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print('\n=== S8.4 BATCH GOLD PROTECTION RESULT ===', flush=True)
        print('OVERALL:', report['status'], flush=True)
        print('REPORT:', out_path, flush=True)
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(run())
