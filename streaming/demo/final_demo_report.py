"""S8.5: summarize existing streaming demo JSON evidence without running Spark.
Usage: python -m streaming.demo.final_demo_report
"""
import json
from datetime import datetime, timezone
from pathlib import Path

DEMO = Path('reports/streaming_demo')
QUALITY = Path('reports/streaming_quality')
OUTPUT = DEMO / 'final'

# Prefer the specifically verified successful runs; do not silently select a failed rerun.
SOURCES = {
    'S8.1 Preflight': (DEMO, 'preflight_20261012T065514Z.json'),
    'S8.2 Validation Gate': (DEMO, 'validation_gate_20261012T065748124664Z.json'),
    'S8.3 Idempotency Precheck': (DEMO, 'idempotency_precheck_20261012T070203819955Z.json'),
    'S8.3 Actual Rerun': (DEMO, 'rerun_20261012T104552795795Z.json'),
    'S8.4 Batch Protection': (DEMO, 'batch_protection_20261012T174843Z.json'),
    'S7 Data Quality': (QUALITY, 'streaming_dq_20261012T105407Z.json'),
    'S7.6 Data Lineage': (QUALITY, 'streaming_lineage_20261012T105553219985Z.json'),
}

def load_source(folder, name):
    path = folder / name
    if not path.is_file():
        return {'file': str(path), 'available': False, 'error': 'File not found'}
    try:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
        return {'file': str(path), 'available': True, 'data': data}
    except (OSError, ValueError) as exc:
        return {'file': str(path), 'available': False, 'error': str(exc)}

def find_named_values(obj, wanted):
    """Return nested values by exact case-insensitive key; evidence, not inference."""
    found = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).lower() == wanted.lower():
                found.append(value)
            if isinstance(value, (dict, list)):
                found.extend(find_named_values(value, wanted))
    elif isinstance(obj, list):
        for value in obj:
            found.extend(find_named_values(value, wanted))
    return found

def summarize(label, source):
    result = {'file': source['file'], 'available': source['available'], 'verdict': 'UNVERIFIED'}
    if not source['available']:
        result['verdict'] = 'MISSING'
        result['error'] = source.get('error')
        return result
    data = source['data']
    result['reported_status'] = {key: data[key] for key in ('status', 'overall', 'summary') if isinstance(data, dict) and key in data}
    checks = data.get('checks', []) if isinstance(data, dict) else []
    if isinstance(checks, list):
        result['checks'] = {'pass': sum(isinstance(c, dict) and str(c.get('status', '')).upper() == 'PASS' for c in checks),
                            'fail': sum(isinstance(c, dict) and str(c.get('status', '')).upper() == 'FAIL' for c in checks)}
    # For S8.4 require explicit evidence of keys, counts, snapshot, and status.
    if label == 'S8.4 Batch Protection':
        by_name = {c.get('name'): c for c in checks if isinstance(c, dict)}
        needed = ('batch_table_exists', 'batch_record_count', 'batch_key_columns',
                  'batch_null_keys', 'batch_duplicate_key_groups', 'batch_snapshot_unchanged')
        keys = by_name.get('batch_key_columns', {}).get('observed')
        valid = (str(data.get('status', '')).upper() == 'PASS' and
                 all(by_name.get(n, {}).get('status') == 'PASS' for n in needed) and
                 keys == ['hour_start', 'location_id'] and
                 by_name['batch_record_count'].get('observed') == 73656 and
                 str(by_name['batch_snapshot_unchanged'].get('observed')) == '5232994262495386058')
        result['verdict'] = 'PASS' if valid else 'REVIEW'
        result['business_keys'] = keys
        result['batch_count'] = by_name.get('batch_record_count', {}).get('observed')
        result['batch_snapshot'] = by_name.get('batch_snapshot_unchanged', {}).get('observed')
        return result
    # Schema-aware verification for each stage, not merely file existence.
    explicit = str(data.get('overall_status', data.get('status', data.get('overall', '')))).upper()
    result['reported_status']['overall_status'] = data.get('overall_status')
    if explicit == 'FAIL':
        result['verdict'] = 'FAIL'
        return result
    if explicit != 'PASS':
        result['verdict'] = 'REVIEW'
        return result

    def checks_all_pass(rows, expected=None, allow_warn=False):
        if not isinstance(rows, list) or not rows:
            return False
        statuses = [str(c.get('status', '')).upper() for c in rows if isinstance(c, dict)]
        if len(statuses) != len(rows):
            return False
        allowed = {'PASS', 'WARN'} if allow_warn else {'PASS'}
        return all(st in allowed for st in statuses) and (expected is None or len(statuses) == expected)

    valid = False
    if label == 'S8.1 Preflight':
        summary = data.get('summary', {})
        counts = data.get('record_counts', {})
        valid = (summary.get('pass') == 14 and summary.get('fail') == 0 and
                 checks_all_pass(checks, 14) and
                 counts.get('silver_stream') == 10 and
                 counts.get('gold_hourly_stream') == 3 and
                 counts.get('gold_integrated_stream') == 3 and
                 counts.get('gold_batch') == 73656)
    elif label == 'S8.2 Validation Gate':
        summary = data.get('summary', {})
        validations = data.get('validations', [])
        by_name = {v.get('name'): v for v in validations if isinstance(v, dict)}
        valid = (summary.get('pass') == 2 and summary.get('fail') == 0 and
                 summary.get('expected_checks') == 2 and len(validations) == 2 and
                 all(by_name.get(n, {}).get('status') == 'PASS' and
                     by_name[n].get('return_code') == 0
                     for n in ('streaming_data_quality', 'streaming_lineage')))
        result['validations'] = {n: {'status': v.get('status'), 'return_code': v.get('return_code')}
                                 for n, v in by_name.items()}
    elif label == 'S8.3 Idempotency Precheck':
        tables = data.get('results', {})
        valid = (isinstance(tables, dict) and
                 set(tables) == {'silver', 'gold_hourly', 'gold_integrated'} and
                 all(tables[t].get('status') == 'PASS' and
                     tables[t].get('duplicate_groups') == 0 and
                     tables[t].get('count_before') == tables[t].get('count_after') == expected and
                     tables[t].get('hash_before') == tables[t].get('hash_after') and
                     all(tables[t].get('checks', {}).get(k) is True for k in
                         ('expected_count', 'unique_keys', 'stable_count', 'stable_content'))
                     for t, expected in (('silver', 10), ('gold_hourly', 3), ('gold_integrated', 3))))
    elif label == 'S7 Data Quality':
        summary = data.get('summary', {})
        statuses = [c.get('status') for c in checks if isinstance(c, dict)]
        valid = (summary.get('PASS') == 31 and summary.get('FAIL') == 0 and
                 summary.get('WARN') == 2 and len(statuses) == 33 and
                 statuses.count('PASS') == 31 and statuses.count('WARN') == 2 and
                 checks_all_pass(checks, 33, allow_warn=True))
        result['quality_summary'] = summary
    elif label == 'S7.6 Data Lineage':
        counts = data.get('record_counts', {})
        bronze = data.get('bronze_objects', [])
        valid = (checks_all_pass(checks, 2) and
                 {c.get('rule_id') for c in checks} ==
                 {'S76-BRONZE-SILVER', 'S76-SILVER-GOLD'} and
                 all(c.get('observed') == 0 for c in checks) and
                 counts.get('bronze') == 10 and counts.get('silver') == 10 and
                 counts.get('gold_hourly') == 3 and len(bronze) == 1 and
                 bronze[0].get('records') == 10 and
                 bronze[0].get('checksum_verified') is True)
    elif label == 'S8.3 Actual Rerun':
        # The source JSON's top-level PASS is retained, but the known physical
        # snapshot change must remain visible in the final report.
        valid = True
        for field in ('logical_idempotency', 'batch_snapshot_protection', 'snapshot_unchanged'):
            values = find_named_values(data, field)
            if values:
                result[field] = values[:3]
    result['verdict'] = 'PASS' if valid else 'REVIEW'

    return result

def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    results = {label: summarize(label, load_source(*spec)) for label, spec in SOURCES.items()}
    # A passing overall needs every source verified. REVIEW requires inspection, not a false PASS.
    verdicts = [item['verdict'] for item in results.values()]
    overall = 'PASS' if all(v == 'PASS' for v in verdicts) else ('FAIL' if 'FAIL' in verdicts else 'REVIEW')
    rerun = results['S8.3 Actual Rerun']
    report = {
        'stage': 'S8.5', 'mode': 'read_only_json_aggregation',
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'overall': overall, 'sources': results,
        'limitations': [
            'Historical Kafka replay is a simulation, not live sensor ingestion.',
            'S8.3 demonstrates logical idempotency; Gold Integrated snapshot changed on rerun, so physical no-op is not established.',
            'S8.4 unchanged Iceberg snapshot supports Batch protection; absent replay-marker columns are not independent contamination checks.',
            'Two partial water-level hours and three missing-tide hours are expected data-quality warnings.',
            'Spark Windows temporary JAR cleanup warnings do not invalidate completed validations.',
        ],
    }
    json_path = OUTPUT / 'final_demo_report.json'
    md_path = OUTPUT / 'final_demo_report.md'
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    lines = [
        '# S8.5 — Streaming Replay Demo: Final Validation Report', '',
        f'- Generated (UTC): {report["created_at_utc"]}',
        f'- Overall automated evidence status: **{overall}**',
        '- Scope: Historical replay Kafka → MinIO Bronze → Iceberg Silver → Gold Hourly → Gold Integrated',
        '- Batch Gold: `lakehouse.gold.hourly_hydrology` (read-only)', '',
        '## Evidence inventory', '', '| Stage | Status | Source file |', '|---|---|---|',
    ]
    for label, item in results.items():
        lines.append(f'| {label} | **{item["verdict"]}** | `{item["file"]}` |')
    lines.extend(['', '## Batch Gold protection', '',
                  '- Expected rows: **73,656**',
                  '- Business key: `hour_start`, `location_id`',
                  '- Expected Iceberg snapshot: `5232994262495386058`',
                  f'- Verified S8.4 status: **{results["S8.4 Batch Protection"]["verdict"]}**', '',
                  '## Idempotency interpretation', '',
                  '- S8.3 rerun supports **logical idempotency** (business content unchanged).',
                  '- Gold Integrated Iceberg snapshot changed in the rerun; **physical no-op is not proven**.',
                  '- Batch Gold snapshot remained unchanged.', '',
                  '## Limitations and warnings', ''])
    lines.extend(f'- {x}' for x in report['limitations'])
    lines.extend(['', '## Next step', '',
                  'If any evidence row is REVIEW or MISSING, inspect the source JSON and update the mapping; do not report an unconditional PASS.', ''])
    md_path.write_text('\n'.join(lines), encoding='utf-8')
    print('=== S8.5 FINAL DEMO REPORT ===')
    for label, item in results.items():
        print(f'[{item["verdict"]}] {label}: {item["file"]}')
    print('OVERALL:', overall)
    print('JSON:', json_path)
    print('MARKDOWN:', md_path)
    return 0 if overall == 'PASS' else 1

if __name__ == '__main__':
    raise SystemExit(main())
