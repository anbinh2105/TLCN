"""Apply Step 11.3 Iceberg CREATE TABLE DDL, without touching Gold Batch.
Run from TLCN root: python -m warehouse.dimensions.apply_warehouse_ddl
"""
from pathlib import Path
from transformation.silver.common import create_spark_session, stop_spark

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / 'warehouse' / 'ddl' / 'warehouse_snowflake_ddl.sql'


def main():
    if not DDL.is_file():
        raise FileNotFoundError(f'Missing DDL: {DDL}')
    sql = '\n'.join(line for line in DDL.read_text(encoding='utf-8-sig').splitlines()
                    if not line.strip().startswith('--'))
    statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
    if not statements or any(not s.upper().startswith(('CREATE NAMESPACE IF NOT EXISTS', 'CREATE TABLE IF NOT EXISTS')) for s in statements):
        raise RuntimeError('Unexpected DDL statement: only CREATE NAMESPACE/TABLE IF NOT EXISTS is allowed')
    spark = create_spark_session(app_name='warehouse-11-3-create-tables', master='local[2]')
    try:
        for statement in statements:
            spark.sql(statement)
            print('[OK]', statement.splitlines()[0][:110])
        print(f'[PASS] Applied {len(statements)} DDL statements')
    finally:
        stop_spark(spark)


if __name__ == '__main__':
    main()
