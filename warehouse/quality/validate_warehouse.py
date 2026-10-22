"""Step 11.6 read-only warehouse quality audit.
Place at warehouse/quality/validate_warehouse.py; run python -m warehouse.quality.validate_warehouse.
Uses Step 11.5 build_frames for exact source-to-fact mapping; no data writes.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import functions as F
from transformation.silver.common import create_spark_session, stop_spark
from warehouse.facts.load_facts import build_frames

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports' / 'warehouse' / 'quality'
NS = 'lakehouse.dw'
SOURCE = 'lakehouse.gold.hourly_hydrology'
EXPECTED = 73656
DIM_KEYS = {
    'dim_year':'year_key','dim_month':'month_key','dim_date':'date_key',
    'dim_time':'time_key','dim_region':'region_key',
    'dim_location':'location_key','dim_station_type':'station_type_key',
    'dim_station':'station_key',
}
FACTS = ('fact_hydrology','fact_water_level','fact_rainfall','fact_data_quality')


def exists(df):
    return df.limit(1).count() > 0


def main():
    spark = None
    report = {'stage':'11.6', 'mode':'read_only', 'generated_at_utc':datetime.now(timezone.utc).isoformat(),
              'source':SOURCE, 'checks':[], 'overall_status':'FAIL'}

    def check(name, actual, expected=True, detail=None):
        ok = actual == expected
        entry = {'name':name, 'status':'PASS' if ok else 'FAIL', 'observed':actual, 'expected':expected}
        if detail is not None: entry['detail'] = detail
        report['checks'].append(entry)
        print(f'[{entry["status"]}] {name}: {actual} (expected {expected})', flush=True)
        return ok

    try:
        spark = create_spark_session(app_name='cantho-warehouse-dq', master='local[2]')
        spark.conf.set('spark.sql.session.timeZone','Asia/Ho_Chi_Minh')
        required = [SOURCE] + [f'{NS}.{n}' for n in DIM_KEYS] + [f'{NS}.{n}' for n in FACTS]
        for name in required:
            check(f'table_exists:{name}', spark.catalog.tableExists(name))
        if any(c['status']=='FAIL' for c in report['checks']):
            raise RuntimeError('Required table missing; cannot continue')

        gold = spark.table(SOURCE)
        check('gold_baseline',gold.count(),EXPECTED)
        check('gold_business_key_nulls',gold.filter(F.col('hour_start').isNull() | F.col('location_id').isNull()).count(),0)
        check('gold_business_key_duplicates',gold.groupBy('hour_start','location_id').count().filter('count > 1').count(),0)
        _, expected_frames = build_frames(gold)
        dims = {name:spark.table(f'{NS}.{name}') for name in DIM_KEYS}
        facts = {name:spark.table(f'{NS}.{name}') for name in FACTS}

        for name,key in DIM_KEYS.items():
            df=dims[name]
            check(f'{name}:key_exists',key in df.columns)
            if key not in df.columns: continue
            check(f'{name}:key_not_null',df.filter(F.col(key).isNull()).count(),0)
            check(f'{name}:key_unique',df.groupBy(key).count().filter('count > 1').count(),0)
        check('dim_year:count',dims['dim_year'].count(),9)
        check('dim_month:count',dims['dim_month'].count(),102)
        check('dim_date:count',dims['dim_date'].count(),3069)
        check('dim_time:count',dims['dim_time'].count(),24)
        check('dim_location:count',dims['dim_location'].count(),1)
        check('dim_station_type:count',dims['dim_station_type'].count(),3)
        check('dim_station:count',dims['dim_station'].count(),3)
        check('dim_region:expected_empty',dims['dim_region'].count(),0, 'Pending verified region metadata')

        def fk(child, child_col, parent, parent_col, label, nullable=False):
            if child_col not in child.columns or parent_col not in parent.columns:
                check(label+':columns_exist',False)
                return
            if not nullable:
                check(label+':not_null',child.filter(F.col(child_col).isNull()).count(),0)
            c=child.filter(F.col(child_col).isNotNull()).select(F.col(child_col).alias('_fk')).distinct()
            p=parent.select(F.col(parent_col).alias('_fk')).distinct()
            check(label+':orphans',c.join(p,'_fk','left_anti').count(),0)

        fk(dims['dim_month'],'year_key',dims['dim_year'],'year_key','dim_month_to_year')
        fk(dims['dim_date'],'month_key',dims['dim_month'],'month_key','dim_date_to_month')
        fk(dims['dim_location'],'region_key',dims['dim_region'],'region_key','dim_location_to_region',nullable=True)
        fk(dims['dim_station'],'station_type_key',dims['dim_station_type'],'station_type_key','dim_station_to_type')
        check('dim_time:hours_0_to_23',dims['dim_time'].filter(~F.col('hour_of_day').between(0,23)).count(),0)

        for name in FACTS:
            df=facts[name]
            check(f'{name}:count',df.count(),EXPECTED)
            check(f'{name}:business_key_nulls',df.filter(F.col('hour_start').isNull() | F.col('location_key').isNull()).count(),0)
            check(f'{name}:business_key_duplicates',df.groupBy('hour_start','location_key').count().filter('count > 1').count(),0)
            check(f'{name}:date_key_matches_timestamp',df.filter(F.col('date_key') != F.date_format('hour_start','yyyyMMdd').cast('int')).count(),0)
            check(f'{name}:time_key_matches_timestamp',df.filter(F.col('time_key') != F.hour('hour_start')).count(),0)
            for col,dim,dimkey in [('date_key','dim_date','date_key'),('time_key','dim_time','time_key'),('location_key','dim_location','location_key')]:
                fk(df,col,dims[dim],dimkey,f'{name}:{col}')
            if 'water_level_station_key' in df.columns:
                fk(df,'water_level_station_key',dims['dim_station'],'station_key',f'{name}:water_level_station_key',nullable=True)
            if 'discharge_station_key' in df.columns:
                fk(df,'discharge_station_key',dims['dim_station'],'station_key',f'{name}:discharge_station_key',nullable=True)
            if 'tide_reference_station_key' in df.columns:
                fk(df,'tide_reference_station_key',dims['dim_station'],'station_key',f'{name}:tide_reference_station_key',nullable=True)

            expected=expected_frames[name]
            check(f'{name}:columns_match_gold_mapping',set(df.columns)==set(expected.columns))
            if set(df.columns)==set(expected.columns):
                expected=expected.select(*df.columns)
                check(f'{name}:data_types_match_gold_mapping',[(f.name,f.dataType.simpleString()) for f in df.schema] == [(f.name,f.dataType.simpleString()) for f in expected.schema])
                # Exact bidirectional multiset comparison, including nulls and duplicate multiplicities.
                if [f.dataType for f in df.schema] == [f.dataType for f in expected.schema]:
                    check(f'{name}:gold_to_fact_missing_rows',expected.exceptAll(df).count(),0)
                    check(f'{name}:fact_to_gold_extra_rows',df.exceptAll(expected).count(),0)

        quality=facts['fact_data_quality']
        for prefix in ['weather','rainfall','discharge','water_level','tide']:
            p,u=prefix+'_present',prefix+'_usable'
            check(f'dq:{prefix}:usable_requires_present',quality.filter(F.col(u).eqNullSafe(True) & ~F.col(p).eqNullSafe(True)).count(),0)
        check('dq:source_count_range',quality.filter(~F.col('present_source_count').between(0,5) | ~F.col('usable_source_count').between(0,5)).count(),0)
        check('dq:usable_not_more_than_present',quality.filter(F.col('usable_source_count') > F.col('present_source_count')).count(),0)
        for kind in ('present','usable'):
            cols=[F.col(f'{p}_{kind}').cast('int') for p in ['weather','rainfall','discharge','water_level','tide']]
            expr=cols[0]
            for c in cols[1:]: expr=expr+c
            check(f'dq:{kind}_count_reconciles',quality.filter(~F.col(f'{kind}_source_count').eqNullSafe(expr)).count(),0)
        report['overall_status']='PASS' if all(c['status']=='PASS' for c in report['checks']) else 'FAIL'
    except Exception as exc:
        report['error']=f'{type(exc).__name__}: {exc}'
        print('[ERROR]',report['error'],flush=True)
    finally:
        OUT.mkdir(parents=True,exist_ok=True)
        path=OUT/('warehouse_dq_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'.json')
        report['check_summary']={s:sum(c['status']==s for c in report['checks']) for s in ('PASS','FAIL')}
        path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print('REPORT:',path)
        print('OVERALL:',report['overall_status'])
        if spark is not None: stop_spark(spark)
    if report['overall_status']!='PASS':
        raise SystemExit(1)

if __name__=='__main__': main()
