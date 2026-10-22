"""Step 11.7: read-only Iceberg snapshot evidence + warehouse documentation.
Run: python -m warehouse.documentation.validate_warehouse_documentation --phase before
     python -m warehouse.facts.load_facts
     python -m warehouse.documentation.validate_warehouse_documentation --phase after
"""
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from transformation.silver.common import create_spark_session, stop_spark

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports' / 'warehouse' / 'documentation'
TABLES = ('dim_year', 'dim_month', 'dim_date', 'dim_time', 'dim_region',
          'dim_location', 'dim_station_type', 'dim_station',
          'fact_hydrology', 'fact_water_level', 'fact_rainfall', 'fact_data_quality')
FACTS = ('fact_hydrology', 'fact_water_level', 'fact_rainfall', 'fact_data_quality')


def snapshots(spark, full):
    # Iceberg metadata table; dim_region can legitimately have no snapshots.
    rows = spark.sql(
        f'SELECT snapshot_id, parent_id, committed_at, operation '
        f'FROM {full}.snapshots ORDER BY committed_at, snapshot_id'
    ).collect()
    return [{'snapshot_id': str(r['snapshot_id']),
             'parent_id': None if r['parent_id'] is None else str(r['parent_id']),
             'committed_at': str(r['committed_at']),
             'operation': r['operation']} for r in rows]


def current_snapshot(spark, full):
    rows = spark.sql(f'SELECT snapshot_id FROM {full}.history '
                     'WHERE is_current_ancestor = true ORDER BY made_current_at DESC LIMIT 1').collect()
    return str(rows[0]['snapshot_id']) if rows else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phase', required=True, choices=('before', 'after'))
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    spark = None
    try:
        spark = create_spark_session(app_name='cantho-warehouse-documentation', master='local[2]')
        snap = {'stage': '11.7', 'phase': args.phase, 'captured_at_utc': datetime.now(timezone.utc).isoformat(),
                'tables': {}}
        dictionary = []
        for name in TABLES:
            full = f'lakehouse.dw.{name}'
            if not spark.catalog.tableExists(full):
                raise RuntimeError(f'Missing table: {full}')
            df = spark.table(full)
            history = snapshots(spark, full)
            latest = current_snapshot(spark, full)
            snap['tables'][name] = {'snapshot_count': len(history),
                                    'current_snapshot_id': latest, 'snapshots': history}
            dictionary.append({'table': full, 'row_count': df.count(),
                               'columns': [{'name': f.name, 'type': f.dataType.simpleString(),
                                            'nullable': f.nullable} for f in df.schema.fields]})
            print(f'[INFO] {name}: rows={dictionary[-1]["row_count"]}, '
                  f'snapshots={len(history)}, current={latest}')
        path = OUT / f'iceberg_snapshots_{args.phase}.json'
        path.write_text(json.dumps(snap, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f'SNAPSHOT EVIDENCE: {path}')
        if args.phase == 'before':
            print('NEXT: rerun python -m warehouse.facts.load_facts; then run --phase after')
            return
        before_path = OUT / 'iceberg_snapshots_before.json'
        if not before_path.exists():
            raise RuntimeError('Missing before snapshot file; run --phase before first')
        before = json.loads(before_path.read_text(encoding='utf-8'))
        comparisons = {}
        for name in FACTS:
            old = before['tables'][name]
            new = snap['tables'][name]
            ok = (old['current_snapshot_id'] == new['current_snapshot_id'] and
                  old['snapshots'] == new['snapshots'])
            comparisons[name] = {'status': 'PASS' if ok else 'FAIL',
                                 'before_count': old['snapshot_count'],
                                 'after_count': new['snapshot_count'],
                                 'before_current_snapshot_id': old['current_snapshot_id'],
                                 'after_current_snapshot_id': new['current_snapshot_id']}
            print(f'[{comparisons[name]["status"]}] {name}: '
                  f'{old["snapshot_count"]} -> {new["snapshot_count"]} snapshots')
        quality_dir = ROOT / 'reports' / 'warehouse' / 'quality'
        quality_reports = sorted(quality_dir.glob('warehouse_dq_*.json')) if quality_dir.exists() else []
        dq = None
        if quality_reports:
            dq = json.loads(quality_reports[-1].read_text(encoding='utf-8'))
        report = {'stage': '11.7', 'mode': 'read_only', 'snapshot_comparison': comparisons,
                  'quality_report_file': str(quality_reports[-1]) if quality_reports else None,
                  'quality_status': dq.get('overall_status') if dq else 'NOT_FOUND',
                  'quality_summary': dq.get('check_summary') if dq else None}
        report['overall_status'] = ('PASS' if all(v['status'] == 'PASS' for v in comparisons.values())
                                    and report['quality_status'] == 'PASS' else 'FAIL')
        (OUT / 'warehouse_validation.json').write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        lines = ['# Data Dictionary — lakehouse.dw', '',
                 'Generated from actual Iceberg table schemas. Nullable describes schema allowance, not observed completeness.', '']
        for item in dictionary:
            lines += [f'## {item["table"]}', '', f'- Row count: {item["row_count"]:,}', '',
                      '| Column | Spark type | Nullable |', '|---|---|---|']
            lines += [f'| `{c["name"]}` | `{c["type"]}` | {c["nullable"]} |' for c in item['columns']]
            lines.append('')
        (OUT / 'data_dictionary.md').write_text('\n'.join(lines), encoding='utf-8')
        lines = ['# Warehouse Validation — Step 11.7', '',
                 '- Source: `lakehouse.gold.hourly_hydrology`',
                 '- Warehouse: `lakehouse.dw` (8 dimensions, 4 facts)',
                 '- Fact grain: `hour_start × location_key`',
                 '- Logical foreign keys are validated by application checks, not enforced by Iceberg.',
                 '- `dim_region` intentionally empty until verified metadata is available.',
                 '- No filling of missing hydrological measurements during warehouse loading.',
                 f'- Step 11.6 status: **{report["quality_status"]}**',
                 f'- Step 11.7 status: **{report["overall_status"]}**', '',
                 '## Snapshot idempotency comparison', '',
                 '| Fact | Before | After | Result |', '|---|---:|---:|---|']
        for name, c in comparisons.items():
            lines.append(f'| `{name}` | {c["before_count"]} | {c["after_count"]} | {c["status"]} |')
        lines += ['', '## Interpretation', '',
                  'Matching current snapshot IDs and full snapshot histories before and after a verified '
                  '`VERIFY_NOOP` rerun support physical no-write idempotency for the observed run.',
                  'This does not establish that the entire pipeline is transactional across tables.', '',
                  '## Evidence files', '',
                  '- `iceberg_snapshots_before.json`', '- `iceberg_snapshots_after.json`',
                  '- `warehouse_validation.json`', '- `data_dictionary.md`',
                  '- Step 11.6 `warehouse_dq_*.json`', '- Step 11.5 rerun `facts_*.json`', '']
        (OUT / 'warehouse_validation.md').write_text('\n'.join(lines), encoding='utf-8')
        print(f'OVERALL: {report["overall_status"]}')
        print(f'DOCUMENTATION: {OUT}')
        if report['overall_status'] != 'PASS':
            raise RuntimeError('Snapshot or warehouse quality validation failed; inspect report')
    finally:
        if spark is not None:
            stop_spark(spark)


if __name__ == '__main__':
    main()