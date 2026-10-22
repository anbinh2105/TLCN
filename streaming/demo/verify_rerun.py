"""S8.3 rerun verification; run from TLCN root.

Each Spark capture executes in its own Python process to avoid reusing a
stopped SparkContext. Use --only-s6 after S4 and S5 have already succeeded.
"""
import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

TABLES = {
    "silver": ("lakehouse.silver.water_level_stream", ["event_id"], 10),
    "gold_hourly": ("lakehouse.gold.water_level_hourly_stream", ["hour_timestamp", "station_code"], 3),
    "gold_integrated": ("lakehouse.gold.hourly_hydrology_stream", ["hour_start", "location_id", "station_code"], 3),
    "batch": ("lakehouse.gold.hourly_hydrology", ["hour_start", "location_id"], 73656),
}
MODULES = [
    "streaming.silver.bronze_to_silver",
    "streaming.gold.silver_to_hourly",
    "streaming.integration.merge_integrated",
]
REPORT_DIR = Path("reports/streaming_demo")


def fingerprint(df):
    columns = sorted(df.columns)
    records = [json.dumps(r.asDict(recursive=True), sort_keys=True, default=str,
                          ensure_ascii=False) for r in df.select(*columns).collect()]
    return hashlib.sha256("\n".join(sorted(records)).encode("utf-8")).hexdigest()


def capture_in_child(output_path):
    from pyspark.sql import functions as F
    from transformation.silver.common import create_spark_session

    spark = create_spark_session("S8-Rerun-Capture-Child")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        result = {}
        for name, (table, keys, expected) in TABLES.items():
            if not spark.catalog.tableExists(table):
                raise RuntimeError(f"Missing table: {table}")
            df = spark.table(table)
            count = df.count()
            if count != expected:
                raise RuntimeError(f"Unexpected count for {table}: {count} != {expected}")
            duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
            if duplicates:
                raise RuntimeError(f"Duplicate keys in {table}: {duplicates}")
            snapshots = spark.sql(
                f"SELECT snapshot_id FROM {table}.snapshots "
                "ORDER BY committed_at DESC LIMIT 1"
            ).collect()
            snapshot_id = str(snapshots[0][0]) if snapshots else None
            result[name] = {
                "table": table,
                "count": count,
                "duplicates": duplicates,
                "snapshot_id": snapshot_id,
                "content_sha256": None if name == "batch" else fingerprint(df),
            }
            print(f"CAPTURE {name}: count={count}, snapshot={snapshot_id}", flush=True)
        # Atomic-ish output: write temporary file, then rename.
        tmp = output_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(output_path)
    finally:
        spark.stop()


def run_capture(label, run_id):
    output = REPORT_DIR / f"capture_{run_id}_{label}.json"
    command = [sys.executable, "-m", "streaming.demo.verify_rerun",
               "--capture-out", str(output)]
    proc = subprocess.run(command, check=False)
    if proc.returncode != 0 or not output.exists():
        raise RuntimeError(f"{label} capture failed (exit={proc.returncode})")
    return json.loads(output.read_text(encoding="utf-8"))


def save_report(report, path):
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--only-s6", action="store_true",
                        help="Run only S6 (when S4/S5 have already succeeded)")
    parser.add_argument("--capture-out", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.capture_out is not None:
        capture_in_child(args.capture_out)
        return
    if not args.execute:
        print("READ-ONLY: no MERGE executed. Use --execute [--only-s6].")
        return

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    report_path = REPORT_DIR / f"rerun_{run_id}.json"
    modules = MODULES[-1:] if args.only_s6 else MODULES
    report = {"stage": "S8.3", "mode": "actual_rerun",
              "scope": "S6_ONLY" if args.only_s6 else "S4_S5_S6",
              "status": "IN_PROGRESS", "steps": []}
    save_report(report, report_path)
    try:
        print("=== BEFORE ===", flush=True)
        report["before"] = run_capture("before", run_id)
        save_report(report, report_path)
        for module in modules:
            print(f"=== RUN {module} ===", flush=True)
            proc = subprocess.run([sys.executable, "-m", module], check=False)
            report["steps"].append({"module": module, "return_code": proc.returncode})
            save_report(report, report_path)
            if proc.returncode != 0:
                raise RuntimeError(f"Module failed: {module} (exit={proc.returncode})")
        print("=== AFTER ===", flush=True)
        report["after"] = run_capture("after", run_id)
        comparisons = {}
        for name in TABLES:
            b, a = report["before"][name], report["after"][name]
            comparisons[name] = {
                "count_unchanged": b["count"] == a["count"],
                "duplicates_zero": a["duplicates"] == 0,
                "content_unchanged": None if name == "batch" else b["content_sha256"] == a["content_sha256"],
                "snapshot_unchanged": b["snapshot_id"] == a["snapshot_id"],
            }
        report["comparisons"] = comparisons
        logical_ok = all(v["count_unchanged"] and v["duplicates_zero"] and
                         v["content_unchanged"] is not False for v in comparisons.values())
        batch_ok = comparisons["batch"]["snapshot_unchanged"]
        report["status"] = "PASS" if logical_ok and batch_ok else "FAIL"
        print("=== S8.3 ACTUAL RERUN RESULT ===")
        print("LOGICAL IDEMPOTENCY:", "PASS" if logical_ok else "FAIL")
        print("BATCH SNAPSHOT PROTECTION:", "PASS" if batch_ok else "FAIL")
        for name, result in comparisons.items():
            print(f"{name}: {result}")
        print("OVERALL:", report["status"])
        if report["status"] != "PASS":
            raise RuntimeError("Rerun verification failed")
    except Exception as exc:
        report["status"] = "FAIL"
        report["error"] = str(exc)
        raise
    finally:
        save_report(report, report_path)
        print("REPORT:", report_path, flush=True)


if __name__ == "__main__":
    main()
