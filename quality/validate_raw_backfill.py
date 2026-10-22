
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ============================================================
# CONFIG
# ============================================================

ROOT = Path("data/raw")
BACKFILL_ROOT = ROOT / "backfill"
REPORT_DIR = Path("reports/gold/quality")

START = pd.Timestamp("2018-04-25 00:00:00")
END = pd.Timestamp("2026-09-18 23:00:00")

SOURCES = {
    "weather": {
        "original": ROOT / "weather" /
            "weather_can_tho_2018_2026.csv",
        "frequency": "h",
        "measurements": [
            "temperature", "humidity", "pressure",
            "wind_speed", "wind_direction",
        ],
        "expected_columns": [
            "timestamp", "temperature", "humidity",
            "pressure", "wind_speed", "wind_direction",
            "latitude", "longitude", "date",
            "year", "month", "day", "hour",
        ],
    },
    "rainfall": {
        "original": ROOT / "rainfall" /
            "rainfall_can_tho_2018_2026.csv",
        "frequency": "h",
        "measurements": ["rainfall_mm"],
        "expected_columns": [
            "timestamp", "rainfall_mm", "latitude",
            "longitude", "date", "year", "month",
            "day", "hour",
        ],
    },
    "river_discharge": {
        "original": ROOT / "river_discharge" /
            "river_discharge_can_tho_2018_2026.csv",
        "frequency": "D",
        "measurements": ["discharge_m3s"],
        "expected_columns": [
            "timestamp", "station", "discharge_m3s",
            "date", "year", "month",
        ],
    },
}


# ============================================================
# HELPERS
# ============================================================

def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(block)

    return digest.hexdigest()


def add_issue(report, level, message):
    report["issues"].append({
        "level": level,
        "message": message,
    })


def expected_windows():
    windows = []

    for year in range(2018, 2027):
        if year > 2018:
            windows.append((
                f"{year}0101",
                f"{year}0424",
            ))

        if year < 2026:
            windows.append((
                f"{year}0919",
                f"{year}1231",
            ))

    return windows


def expected_backfill_paths(source):
    return [
        BACKFILL_ROOT / source /
        f"{source}_backfill_{start}_{end}.csv"
        for start, end in expected_windows()
    ]


def inspect_metadata(path, report):
    metadata_path = path.with_suffix(".metadata.json")

    if not metadata_path.exists():
        add_issue(
            report, "WARNING",
            f"Missing metadata: {metadata_path}"
        )
        return

    try:
        with metadata_path.open(
            encoding="utf-8"
        ) as stream:
            metadata = json.load(stream)

        expected_hash = metadata.get("sha256")
        actual_hash = sha256_file(path)

        if not expected_hash:
            add_issue(
                report, "WARNING",
                f"Missing SHA-256 in {metadata_path}"
            )
        elif expected_hash != actual_hash:
            add_issue(
                report, "ERROR",
                f"Checksum mismatch: {path}"
            )

        if metadata.get("status") != "DOWNLOADED":
            add_issue(
                report, "WARNING",
                f"Unexpected metadata status: {path}"
            )

        api = metadata.get("api_metadata", {})

        for key in (
            "api_url", "request_params",
            "response_timezone",
            "response_latitude",
            "response_longitude",
        ):
            if api.get(key) is None:
                add_issue(
                    report, "WARNING",
                    f"Missing metadata {key}: {path}"
                )

        params = api.get("request_params", {})

        if params.get("timezone") != "Asia/Ho_Chi_Minh":
            add_issue(
                report, "ERROR",
                f"Wrong request timezone: {path}"
            )

        if api.get("response_timezone") != "Asia/Ho_Chi_Minh":
            add_issue(
                report, "ERROR",
                f"Wrong response timezone: {path}"
            )

    except Exception as exc:
        add_issue(
            report, "ERROR",
            f"Invalid metadata {metadata_path}: {exc}"
        )


# ============================================================
# VALIDATE SOURCE
# ============================================================

def validate_source(source, config):
    print("\n" + "=" * 65)
    print(f"VALIDATING: {source.upper()}")
    print("=" * 65)

    report = {
        "source": source,
        "status": "PENDING",
        "files": [],
        "issues": [],
        "total_rows": 0,
        "unique_timestamps": 0,
        "duplicate_timestamps": 0,
        "missing_timestamps": 0,
        "extra_timestamps": 0,
        "null_measurements": {},
    }

    original = config["original"]
    backfill_paths = expected_backfill_paths(source)
    paths = [original] + backfill_paths

    print("Expected files:", len(paths))

    all_timestamps = []
    total_nulls = {
        col: 0 for col in config["measurements"]
    }

    for index, path in enumerate(paths):
        if not path.exists():
            add_issue(
                report, "ERROR",
                f"Missing file: {path}"
            )
            continue

        try:
            df = pd.read_csv(
                path,
                encoding="utf-8-sig",
                low_memory=False,
            )

            # Require the exact expected schema.
            if df.columns.tolist() != config["expected_columns"]:
                add_issue(
                    report, "ERROR",
                    f"Schema mismatch: {path}. "
                    f"Actual columns: {df.columns.tolist()}"
                )

            if "timestamp" not in df.columns:
                continue

            timestamps = pd.to_datetime(
                df["timestamp"],
                errors="coerce",
            )

            invalid_count = int(timestamps.isna().sum())

            if invalid_count:
                add_issue(
                    report, "ERROR",
                    f"Invalid timestamps ({invalid_count}): {path}"
                )

            # Backfill file names contain their requested dates.
            if index > 0:
                match = re.search(
                    r"_backfill_(\d{8})_(\d{8})\.csv$",
                    path.name,
                )

                if match:
                    window_start = pd.Timestamp(
                        match.group(1)
                    )
                    window_end = pd.Timestamp(
                        match.group(2)
                    )

                    valid = timestamps.dropna()

                    if (
                        (valid < window_start).any()
                        or
                        (valid >= window_end + pd.Timedelta(days=1)).any()
                    ):
                        add_issue(
                            report, "ERROR",
                            f"Out-of-window timestamp: {path}"
                        )

                inspect_metadata(path, report)

            # Check date-derived columns.
            valid_mask = timestamps.notna()
            valid_ts = timestamps[valid_mask]

            expected_derived = {
                "date": valid_ts.dt.strftime("%Y-%m-%d"),
                "year": valid_ts.dt.year,
                "month": valid_ts.dt.month,
            }

            if config["frequency"] == "h":
                expected_derived.update({
                    "day": valid_ts.dt.day,
                    "hour": valid_ts.dt.hour,
                })

            for col, expected_values in expected_derived.items():
                if col not in df.columns:
                    continue

                if col == "date":
                    actual_values = pd.to_datetime(
                        df.loc[valid_mask, col],
                        errors="coerce",
                    ).dt.strftime("%Y-%m-%d")
                else:
                    actual_values = pd.to_numeric(
                        df.loc[valid_mask, col],
                        errors="coerce",
                    )

                mismatch = int(
                    (~(
                        actual_values.reset_index(drop=True)
                        == expected_values.reset_index(drop=True)
                    )).sum()
                )

                if mismatch:
                    add_issue(
                        report, "ERROR",
                        f"Derived column {col} mismatch "
                        f"({mismatch} rows): {path}"
                    )

            file_nulls = {}

            for col in config["measurements"]:
                if col not in df.columns:
                    add_issue(
                        report, "ERROR",
                        f"Missing measurement {col}: {path}"
                    )
                    continue

                numeric = pd.to_numeric(
                    df[col], errors="coerce"
                )

                null_count = int(numeric.isna().sum())

                file_nulls[col] = null_count
                total_nulls[col] += null_count

            # Check simple physical plausibility.
            if source == "rainfall" and "rainfall_mm" in df:
                values = pd.to_numeric(
                    df["rainfall_mm"], errors="coerce"
                )
                if (values < 0).any():
                    add_issue(
                        report, "ERROR",
                        f"Negative rainfall: {path}"
                    )

            if source == "river_discharge" and "discharge_m3s" in df:
                values = pd.to_numeric(
                    df["discharge_m3s"], errors="coerce"
                )
                if (values < 0).any():
                    add_issue(
                        report, "WARNING",
                        f"Negative discharge: {path}"
                    )

            if source == "weather":
                if "humidity" in df:
                    values = pd.to_numeric(
                        df["humidity"], errors="coerce"
                    )
                    if ((values < 0) | (values > 100)).any():
                        add_issue(
                            report, "ERROR",
                            f"Humidity outside 0-100%: {path}"
                        )

            report["total_rows"] += len(df)
            all_timestamps.append(timestamps)

            report["files"].append({
                "path": str(path),
                "kind": (
                    "ORIGINAL" if index == 0 else "BACKFILL"
                ),
                "rows": len(df),
                "invalid_timestamps": invalid_count,
                "null_measurements": file_nulls,
                "sha256": sha256_file(path),
            })

            print(
                f"{path.name}: "
                f"{len(df):,} rows | "
                f"invalid_ts={invalid_count}"
            )

        except Exception as exc:
            add_issue(
                report, "ERROR",
                f"Could not validate {path}: {exc}"
            )

    # ========================================================
    # GLOBAL TEMPORAL RECONCILIATION
    # ========================================================

    if all_timestamps:
        combined = pd.concat(
            all_timestamps, ignore_index=True
        )

        valid = pd.DatetimeIndex(
            combined.dropna()
        )

        report["unique_timestamps"] = int(
            valid.nunique()
        )

        report["duplicate_timestamps"] = int(
            valid.duplicated().sum()
        )

        if report["duplicate_timestamps"]:
            add_issue(
                report, "ERROR",
                "Duplicate timestamps across original/backfill"
            )

        expected_start = START

        expected_end = (
            END if config["frequency"] == "h"
            else END.normalize()
        )

        expected = pd.date_range(
            expected_start.normalize()
            if config["frequency"] == "D"
            else expected_start,
            expected_end,
            freq=config["frequency"],
        )

        actual_unique = valid.unique()

        missing = expected.difference(actual_unique)
        extra = actual_unique.difference(expected)

        report["expected_timestamps"] = len(expected)
        report["missing_timestamps"] = len(missing)
        report["extra_timestamps"] = len(extra)

        report["missing_examples"] = [
            str(x) for x in missing[:10]
        ]

        report["extra_examples"] = [
            str(x) for x in extra[:10]
        ]

        if len(missing):
            add_issue(
                report, "ERROR",
                f"Missing {len(missing)} timestamps"
            )

        if len(extra):
            add_issue(
                report, "ERROR",
                f"Extra {len(extra)} timestamps"
            )

    else:
        add_issue(
            report, "ERROR",
            "No timestamps available"
        )

    report["null_measurements"] = total_nulls

    if any(total_nulls.values()):
        add_issue(
            report, "WARNING",
            "Null or non-numeric measurement values exist"
        )

    levels = [
        item["level"] for item in report["issues"]
    ]

    if "ERROR" in levels:
        report["status"] = "FAIL"
    elif "WARNING" in levels:
        report["status"] = "PASS_WITH_WARNINGS"
    else:
        report["status"] = "PASS"

    print("\nRESULT:", report["status"])
    print("Rows:", report["total_rows"])
    print("Unique timestamps:", report["unique_timestamps"])
    print("Missing timestamps:", report["missing_timestamps"])
    print("Duplicate timestamps:", report["duplicate_timestamps"])
    print("Null measurements:", total_nulls)

    for issue in report["issues"]:
        print(
            f"[{issue['level']}] {issue['message']}"
        )

    return report


# ============================================================
# MAIN
# ============================================================

def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    results = {
        "analysis": "raw_backfill_validation",
        "validated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "sources": {},
    }

    for source, config in SOURCES.items():
        results["sources"][source] = validate_source(
            source, config
        )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    report_path = REPORT_DIR / (
        f"raw_backfill_validation_{timestamp}.json"
    )

    with report_path.open(
        "x", encoding="utf-8"
    ) as stream:
        json.dump(
            results, stream,
            indent=2,
            ensure_ascii=False,
        )

    statuses = {
        source: result["status"]
        for source, result in results["sources"].items()
    }

    print("\n" + "=" * 65)
    print("RAW BACKFILL VALIDATION SUMMARY")
    print("=" * 65)

    for source, status in statuses.items():
        print(f"{source}: {status}")

    print("Report:", report_path)

    if "FAIL" in statuses.values():
        print("RAW BACKFILL VALIDATION: FAIL")
        raise SystemExit(1)

    if "PASS_WITH_WARNINGS" in statuses.values():
        print("RAW BACKFILL VALIDATION: PASS WITH WARNINGS")
    else:
        print("RAW BACKFILL VALIDATION: PASS")


if __name__ == "__main__":
    main()
