from __future__ import annotations

import io
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
import yaml
from dotenv import load_dotenv
from minio import Minio


# ============================================================
# 1. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

QUALITY_RULES_PATH = PROJECT_ROOT / "config" / "quality_rules.yaml"
INGESTION_LOG_PATH = PROJECT_ROOT / "data" / "metadata" / "ingestion_log.jsonl"
LOCAL_REPORT_ROOT = PROJECT_ROOT / "reports" / "quality"


# ============================================================
# 2. ENVIRONMENT / MINIO
# ============================================================

load_dotenv(PROJECT_ROOT / ".env")

import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "flood-lakehouse")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError(
        "Missing MINIO_ACCESS_KEY or MINIO_SECRET_KEY in .env"
    )


minio_client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


# ============================================================
# 3. GENERAL HELPERS
# ============================================================

def now_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def load_quality_rules() -> dict[str, Any]:
    """Load quality rules from YAML."""
    if not QUALITY_RULES_PATH.exists():
        raise FileNotFoundError(
            f"Quality rules not found: {QUALITY_RULES_PATH}"
        )

    with open(QUALITY_RULES_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not config or "sources" not in config:
        raise ValueError(
            "quality_rules.yaml must contain top-level key 'sources'"
        )

    return config["sources"]


def load_successful_bronze_objects() -> list[dict[str, Any]]:
    """
    Load unique successfully ingested Bronze objects.

    SKIPPED records are ignored because they refer to content
    already successfully ingested earlier.
    """
    if not INGESTION_LOG_PATH.exists():
        raise FileNotFoundError(
            f"Ingestion log not found: {INGESTION_LOG_PATH}"
        )

    unique_objects: dict[str, dict[str, Any]] = {}

    with open(INGESTION_LOG_PATH, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[WARNING] Invalid JSON at ingestion log "
                    f"line {line_number}: {exc}"
                )
                continue

            if record.get("status") != "SUCCESS":
                continue

            object_path = record.get("object_path")

            if not object_path:
                print(
                    f"[WARNING] SUCCESS record without object_path "
                    f"at line {line_number}"
                )
                continue

            unique_objects[object_path] = record

    return list(unique_objects.values())


def download_object_bytes(object_path: str) -> bytes:
    """Download one object from MinIO into memory."""
    response = None

    try:
        response = minio_client.get_object(
            MINIO_BUCKET,
            object_path,
        )
        return response.read()

    finally:
        if response is not None:
            response.close()
            response.release_conn()


# ============================================================
# 4. READ BRONZE FILE
# ============================================================

def detect_csv_delimiter(data: bytes) -> str:
    """
    Detect common CSV delimiters.

    Falls back to comma if detection is inconclusive.
    """
    sample = data[:8192]

    text = None

    for encoding in ("utf-8-sig", "utf-8", "cp1258", "latin1"):
        try:
            text = sample.decode(encoding)
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        return ","

    first_line = text.splitlines()[0] if text.splitlines() else ""

    candidates = [",", ";", "\t", "|"]

    counts = {
        delimiter: first_line.count(delimiter)
        for delimiter in candidates
    }

    best = max(counts, key=counts.get)

    if counts[best] == 0:
        return ","

    return best


def read_csv_bytes(
    data: bytes,
    delimiter_config: str,
) -> pd.DataFrame:
    """Read CSV bytes without cleaning or transforming values."""

    if delimiter_config == "auto":
        delimiter = detect_csv_delimiter(data)
    else:
        delimiter = delimiter_config

    last_error = None

    for encoding in ("utf-8-sig", "utf-8", "cp1258", "latin1"):
        try:
            return pd.read_csv(
                io.BytesIO(data),
                sep=delimiter,
                encoding=encoding,
            )
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error:
        raise last_error

    raise ValueError("Unable to read CSV file")


def read_bronze_dataframe(
    data: bytes,
    object_path: str,
    rules: dict[str, Any],
) -> pd.DataFrame:
    """Read CSV/XLSX Bronze object into a DataFrame."""

    suffix = Path(object_path).suffix.lower()

    allowed_types = [
        item.lower()
        for item in rules.get("file_types", [])
    ]

    if suffix not in allowed_types:
        raise ValueError(
            f"Unsupported file type '{suffix}'. "
            f"Allowed: {allowed_types}"
        )

    if suffix == ".csv":
        delimiter = rules.get("csv", {}).get(
            "delimiter",
            ",",
        )

        return read_csv_bytes(
            data=data,
            delimiter_config=delimiter,
        )

    if suffix == ".xlsx":
        return pd.read_excel(
            io.BytesIO(data),
            engine="openpyxl",
        )

    raise ValueError(f"Unsupported file type: {suffix}")


# ============================================================
# 5. GREAT EXPECTATIONS
# ============================================================

def run_ge_checks(
    df: pd.DataFrame,
    required_columns: list[str],
) -> dict[str, Any]:
    """
    Run structural checks with Great Expectations.

    GE responsibilities here:
      - table contains at least one row
      - required columns exist
    """

    result = {
        "file_not_empty": {
            "success": False,
            "observed_row_count": int(len(df)),
        },
        "required_columns_exist": {
            "success": False,
            "missing_columns": [],
            "details": {},
        },
    }

    # --------------------------------------------------------
    # GE 1.x ephemeral context + dataframe datasource
    # --------------------------------------------------------

    context = gx.get_context(mode="ephemeral")

    datasource_name = f"bronze_ds_{uuid.uuid4().hex}"
    asset_name = f"bronze_asset_{uuid.uuid4().hex}"
    batch_definition_name = f"bronze_batch_{uuid.uuid4().hex}"
    suite_name = f"bronze_suite_{uuid.uuid4().hex}"

    datasource = context.data_sources.add_pandas(
        name=datasource_name
    )

    asset = datasource.add_dataframe_asset(
        name=asset_name
    )

    batch_definition = asset.add_batch_definition_whole_dataframe(
        name=batch_definition_name
    )

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": df
        }
    )

    suite = gx.ExpectationSuite(
        name=suite_name
    )

    suite = context.suites.add(suite)

    validator = context.get_validator(
        batch=batch,
        expectation_suite=suite,
    )

    # --------------------------------------------------------
    # GE: file/table not empty
    # --------------------------------------------------------

    ge_non_empty = validator.expect_table_row_count_to_be_between(
        min_value=1
    )

    result["file_not_empty"]["success"] = bool(
        ge_non_empty.success
    )

    # --------------------------------------------------------
    # GE: required columns
    # --------------------------------------------------------

    missing_columns = []

    for column in required_columns:
        ge_column = validator.expect_column_to_exist(
            column=column
        )

        success = bool(ge_column.success)

        result["required_columns_exist"]["details"][column] = {
            "success": success
        }

        if not success:
            missing_columns.append(column)

    result["required_columns_exist"]["missing_columns"] = (
        missing_columns
    )

    result["required_columns_exist"]["success"] = (
        len(missing_columns) == 0
    )

    return result


# ============================================================
# 6. CUSTOM CRITICAL CHECKS
# ============================================================

def check_timestamp_parseable(
    df: pd.DataFrame,
    columns: list[str],
) -> dict[str, Any]:

    details = {}
    total_invalid = 0

    for column in columns:

        if column not in df.columns:
            details[column] = {
                "success": False,
                "reason": "COLUMN_NOT_FOUND",
            }
            total_invalid += 1
            continue

        series = df[column]

        parsed = pd.to_datetime(
            series,
            errors="coerce",
        )

        invalid_mask = (
            series.notna()
            & parsed.isna()
        )

        invalid_count = int(invalid_mask.sum())

        total_invalid += invalid_count

        details[column] = {
            "success": invalid_count == 0,
            "invalid_count": invalid_count,
        }

    return {
        "success": total_invalid == 0,
        "invalid_count": total_invalid,
        "details": details,
    }


def check_numeric_parseable(
    df: pd.DataFrame,
    columns: list[str],
) -> dict[str, Any]:

    details = {}
    total_invalid = 0

    for column in columns:

        if column not in df.columns:
            details[column] = {
                "success": False,
                "reason": "COLUMN_NOT_FOUND",
            }
            total_invalid += 1
            continue

        series = df[column]

        parsed = pd.to_numeric(
            series,
            errors="coerce",
        )

        invalid_mask = (
            series.notna()
            & parsed.isna()
        )

        invalid_count = int(invalid_mask.sum())

        total_invalid += invalid_count

        details[column] = {
            "success": invalid_count == 0,
            "invalid_count": invalid_count,
        }

    return {
        "success": total_invalid == 0,
        "invalid_count": total_invalid,
        "details": details,
    }


def check_tide_date_parseable(
    df: pd.DataFrame,
    column: str,
) -> dict[str, Any]:

    if column not in df.columns:
        return {
            "success": False,
            "invalid_count": None,
            "reason": "COLUMN_NOT_FOUND",
        }

    series = df[column]

    parsed = pd.to_datetime(
        series,
        errors="coerce",
    )

    invalid_mask = (
        series.notna()
        & parsed.isna()
    )

    invalid_count = int(invalid_mask.sum())

    return {
        "success": invalid_count == 0,
        "invalid_count": invalid_count,
    }


def parse_time_value(value: Any) -> bool:
    """
    Validate whether a raw Tide 'Giờ' value represents a valid time.

    Supported raw representations:
      1. datetime.time
      2. datetime / pandas Timestamp
      3. integer/float hour: 0..23
      4. Excel fractional-day time: 0 <= value < 1
      5. strings: HH:MM, HH:MM:SS
      6. numeric strings: "0".."23"
    """

    if pd.isna(value):
        return True

    # --------------------------------------------------------
    # 1. Python datetime/time-like object
    # --------------------------------------------------------
    if hasattr(value, "hour") and hasattr(value, "minute"):
        try:
            hour = int(value.hour)
            minute = int(value.minute)
            second = int(getattr(value, "second", 0))

            return (
                0 <= hour <= 23
                and 0 <= minute <= 59
                and 0 <= second <= 59
            )
        except (TypeError, ValueError):
            return False

    # --------------------------------------------------------
    # 2. Numeric representation
    # --------------------------------------------------------
    if isinstance(value, (int, float)):

        numeric_value = float(value)

        # Dataset-specific representation:
        # 0 -> 00:00, 1 -> 01:00, ..., 23 -> 23:00
        if numeric_value.is_integer():
            hour = int(numeric_value)

            if 0 <= hour <= 23:
                return True

        # Standard Excel fractional-day representation
        # e.g. 0.5 = 12:00
        if 0 <= numeric_value < 1:
            return True

        return False

    # --------------------------------------------------------
    # 3. String representation
    # --------------------------------------------------------
    text = str(value).strip()

    if not text:
        return True

    # Numeric string such as "0", "1", ..., "23"
    try:
        numeric_value = float(text)

        if numeric_value.is_integer():
            hour = int(numeric_value)

            if 0 <= hour <= 23:
                return True

        if 0 <= numeric_value < 1:
            return True

    except ValueError:
        pass

    # Normal HH:MM / HH:MM:SS
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            datetime.strptime(text, fmt)
            return True
        except ValueError:
            pass

    return False


def check_tide_time_parseable(
    df: pd.DataFrame,
    column: str,
) -> dict[str, Any]:

    if column not in df.columns:
        return {
            "success": False,
            "invalid_count": None,
            "reason": "COLUMN_NOT_FOUND",
        }

    invalid_count = 0

    for value in df[column]:
        if not parse_time_value(value):
            invalid_count += 1

    return {
        "success": invalid_count == 0,
        "invalid_count": invalid_count,
    }


# ============================================================
# 7. WARNING CHECKS
# ============================================================

def check_nulls(
    df: pd.DataFrame,
    columns: list[str],
) -> dict[str, Any]:

    details = {}
    total_nulls = 0

    for column in columns:

        if column not in df.columns:
            continue

        null_count = int(
            df[column].isna().sum()
        )

        null_pct = (
            (null_count / len(df) * 100)
            if len(df) > 0
            else 0.0
        )

        total_nulls += null_count

        details[column] = {
            "null_count": null_count,
            "null_pct": round(null_pct, 4),
        }

    return {
        "has_warning": total_nulls > 0,
        "total_nulls": total_nulls,
        "details": details,
    }


def check_duplicate_keys(
    df: pd.DataFrame,
    key_columns: list[str],
) -> dict[str, Any]:

    missing = [
        column
        for column in key_columns
        if column not in df.columns
    ]

    if missing:
        return {
            "has_warning": True,
            "duplicate_count": None,
            "reason": "KEY_COLUMN_NOT_FOUND",
            "missing_columns": missing,
        }

    duplicate_mask = df.duplicated(
        subset=key_columns,
        keep="first",
    )

    duplicate_count = int(
        duplicate_mask.sum()
    )

    return {
        "has_warning": duplicate_count > 0,
        "duplicate_count": duplicate_count,
        "key_columns": key_columns,
    }


def get_duplicate_key_columns(
    source: str,
    rules: dict[str, Any],
) -> list[str]:

    if source == "weather":
        return ["timestamp"]

    if source == "rainfall":
        return ["timestamp"]

    if source == "river_discharge":
        return ["station", "timestamp"]

    if source == "water_level":
        return [
            "Station Code",
            "Timestamp (UTC+07:00)",
        ]

    if source == "tide":
        return [
            rules["date_column"],
            rules["time_column"],
        ]

    return []


# ============================================================
# 8. VALIDATE ONE BRONZE OBJECT
# ============================================================

def validate_object(
    ingestion_record: dict[str, Any],
    all_rules: dict[str, Any],
) -> dict[str, Any]:

    source = ingestion_record["source"]
    object_path = ingestion_record["object_path"]

    if source not in all_rules:
        raise ValueError(
            f"No quality rules configured for source '{source}'"
        )

    rules = all_rules[source]

    print(f"\nValidating: {object_path}")

    # --------------------------------------------------------
    # Download + read
    # --------------------------------------------------------

    raw_bytes = download_object_bytes(
        object_path
    )

    df = read_bronze_dataframe(
        data=raw_bytes,
        object_path=object_path,
        rules=rules,
    )

    # --------------------------------------------------------
    # Great Expectations structural checks
    # --------------------------------------------------------

    required_columns = rules.get(
        "required_columns",
        [],
    )

    ge_results = run_ge_checks(
        df=df,
        required_columns=required_columns,
    )

    critical = {
        "file_not_empty":
            ge_results["file_not_empty"],

        "required_columns_exist":
            ge_results["required_columns_exist"],
    }

    # --------------------------------------------------------
    # Timestamp checks
    # --------------------------------------------------------

    if (
        rules.get("critical_rules", {})
        .get("timestamp_parseable", False)
    ):
        critical["timestamp_parseable"] = (
            check_timestamp_parseable(
                df,
                rules.get(
                    "timestamp_columns",
                    [],
                ),
            )
        )

    # --------------------------------------------------------
    # Tide date/time checks
    # --------------------------------------------------------

    if (
        rules.get("critical_rules", {})
        .get("date_parseable", False)
    ):
        critical["date_parseable"] = (
            check_tide_date_parseable(
                df,
                rules["date_column"],
            )
        )

    if (
        rules.get("critical_rules", {})
        .get("time_parseable", False)
    ):
        critical["time_parseable"] = (
            check_tide_time_parseable(
                df,
                rules["time_column"],
            )
        )

    # --------------------------------------------------------
    # Numeric checks
    # --------------------------------------------------------

    if (
        rules.get("critical_rules", {})
        .get("numeric_parseable", False)
    ):
        critical["numeric_parseable"] = (
            check_numeric_parseable(
                df,
                rules.get(
                    "numeric_columns",
                    [],
                ),
            )
        )

    # --------------------------------------------------------
    # Warnings
    # --------------------------------------------------------

    warnings = {}

    warning_rules = rules.get(
        "warning_rules",
        {},
    )

    if warning_rules.get(
        "check_nulls",
        False,
    ):
        columns_to_check = list(
            dict.fromkeys(
                required_columns
                + rules.get("numeric_columns", [])
                + rules.get("timestamp_columns", [])
            )
        )

        if source == "tide":
            columns_to_check.extend(
                [
                    rules["date_column"],
                    rules["time_column"],
                ]
            )

            columns_to_check = list(
                dict.fromkeys(columns_to_check)
            )

        warnings["nulls"] = check_nulls(
            df,
            columns_to_check,
        )

    if (
        warning_rules.get(
            "check_duplicate_timestamp",
            False,
        )
        or warning_rules.get(
            "check_duplicate_datetime",
            False,
        )
    ):
        key_columns = get_duplicate_key_columns(
            source,
            rules,
        )

        warnings["duplicates"] = (
            check_duplicate_keys(
                df,
                key_columns,
            )
        )

    # --------------------------------------------------------
    # Final PASS / FAIL
    # --------------------------------------------------------

    critical_failures = [
        name
        for name, result in critical.items()
        if not result.get("success", False)
    ]

    quality_status = (
        "PASS"
        if len(critical_failures) == 0
        else "FAIL"
    )

    warning_count = sum(
        1
        for result in warnings.values()
        if result.get("has_warning", False)
    )

    return {
        "batch_id": ingestion_record.get("batch_id"),
        "source": source,
        "source_file": ingestion_record.get(
            "source_file"
        ),
        "object_path": object_path,
        "checksum": ingestion_record.get(
            "checksum"
        ),
        "validation_timestamp": now_iso(),
        "validation_engine":
            "Great Expectations 1.x + Custom Checks",
        "record_count": int(len(df)),
        "critical": critical,
        "critical_failed": critical_failures,
        "warnings": warnings,
        "warning_count": warning_count,
        "quality_status": quality_status,
    }


# ============================================================
# 9. QUARANTINE
# ============================================================

def build_quarantine_path(
    report: dict[str, Any],
) -> str:

    object_path = report["object_path"]

    if object_path.startswith("bronze/"):
        return object_path.replace(
            "bronze/",
            "quarantine/",
            1,
        )

    return (
        f"quarantine/"
        f"{report['source']}/"
        f"batch_id={report['batch_id']}/"
        f"{report['source_file']}"
    )


def copy_to_quarantine(
    report: dict[str, Any],
) -> str:
    """
    Copy failed Bronze object to quarantine.

    Bronze object is NOT deleted.
    """

    source_object = report["object_path"]

    quarantine_object = build_quarantine_path(
        report
    )

    # Use server-side copy inside MinIO
    from minio.commonconfig import CopySource

    minio_client.copy_object(
        bucket_name=MINIO_BUCKET,
        object_name=quarantine_object,
        source=CopySource(
            MINIO_BUCKET,
            source_object,
        ),
    )

    return quarantine_object


# ============================================================
# 10. QUALITY REPORT
# ============================================================

def safe_filename(value: str) -> str:
    """Create safe report filename."""
    return (
        value
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )


def write_local_report(
    report: dict[str, Any],
) -> Path:

    batch_id = str(
        report.get("batch_id", "unknown_batch")
    )

    report_dir = (
        LOCAL_REPORT_ROOT
        / batch_id
        / report["source"]
    )

    report_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    filename = (
        safe_filename(
            report["source_file"]
            or Path(report["object_path"]).name
        )
        + ".quality.json"
    )

    report_path = report_dir / filename

    with open(
        report_path,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            report,
            f,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    return report_path


def upload_quality_report(
    report: dict[str, Any],
) -> str:

    batch_id = str(
        report.get("batch_id", "unknown_batch")
    )

    source = report["source"]

    source_file = (
        report.get("source_file")
        or Path(report["object_path"]).name
    )

    report_object = (
        f"quality-reports/"
        f"{source}/"
        f"batch_id={batch_id}/"
        f"{safe_filename(source_file)}.quality.json"
    )

    payload = json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
        default=str,
    ).encode("utf-8")

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=report_object,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    return report_object


# ============================================================
# 11. MAIN
# ============================================================

def main() -> None:

    print("=" * 70)
    print("BRONZE DATA QUALITY VALIDATION")
    print("=" * 70)

    print(f"Bucket     : {MINIO_BUCKET}")
    print(f"Endpoint   : {MINIO_ENDPOINT}")
    print(f"Rules      : {QUALITY_RULES_PATH}")
    print(f"Ingest log : {INGESTION_LOG_PATH}")

    # --------------------------------------------------------
    # Basic checks
    # --------------------------------------------------------

    if not minio_client.bucket_exists(
        MINIO_BUCKET
    ):
        raise RuntimeError(
            f"MinIO bucket does not exist: "
            f"{MINIO_BUCKET}"
        )

    rules = load_quality_rules()

    bronze_objects = (
        load_successful_bronze_objects()
    )

    print(
        f"\nSuccessful Bronze objects found: "
        f"{len(bronze_objects)}"
    )

    if not bronze_objects:
        print(
            "\nNo SUCCESS Bronze objects found. "
            "Run ingestion first."
        )
        return

    # --------------------------------------------------------
    # Counters
    # --------------------------------------------------------

    pass_count = 0
    fail_count = 0
    error_count = 0
    files_with_warnings = 0

    summary_records = []

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    for index, ingestion_record in enumerate(
        bronze_objects,
        start=1,
    ):
        object_path = ingestion_record.get(
            "object_path",
            "UNKNOWN"
        )

        print(
            f"\n[{index}/{len(bronze_objects)}] "
            f"{object_path}"
        )

        try:
            report = validate_object(
                ingestion_record,
                rules,
            )

            if report["quality_status"] == "FAIL":

                quarantine_path = (
                    copy_to_quarantine(report)
                )

                report["quarantine_path"] = (
                    quarantine_path
                )

                fail_count += 1

                print(
                    "  -> QUALITY: FAIL"
                )

                print(
                    "  -> Critical failures:",
                    ", ".join(
                        report["critical_failed"]
                    ),
                )

                print(
                    f"  -> Quarantine: "
                    f"{quarantine_path}"
                )

            else:
                report["quarantine_path"] = None
                pass_count += 1

                print(
                    "  -> QUALITY: PASS"
                )

            if report["warning_count"] > 0:
                files_with_warnings += 1

                print(
                    f"  -> Warnings: "
                    f"{report['warning_count']}"
                )

            # Write after quarantine path has been added
            local_report_path = write_local_report(
                report
            )

            minio_report_path = upload_quality_report(
                report
            )

            print(
                f"  -> Local report: "
                f"{local_report_path}"
            )

            print(
                f"  -> MinIO report: "
                f"{minio_report_path}"
            )

            summary_records.append(
                {
                    "batch_id": report["batch_id"],
                    "source": report["source"],
                    "source_file": report["source_file"],
                    "object_path": report["object_path"],
                    "record_count": report["record_count"],

                    # Full quality details
                    "critical": report["critical"],
                    "critical_failed": report["critical_failed"],

                    "warnings": report["warnings"],
                    "warning_count": report["warning_count"],

                    "quality_status": report["quality_status"],
                    "quarantine_path": report["quarantine_path"],
                }
            )

        except Exception as exc:
            error_count += 1

            print(
                f"  -> VALIDATION ERROR: "
                f"{type(exc).__name__}: {exc}"
            )

            summary_records.append(
                {
                    "source":
                        ingestion_record.get("source"),
                    "source_file":
                        ingestion_record.get(
                            "source_file"
                        ),
                    "object_path":
                        object_path,
                    "quality_status":
                        "ERROR",
                    "critical_failed": [],
                    "warning_count": 0,
                    "quarantine_path": None,
                    "error":
                        f"{type(exc).__name__}: {exc}",
                }
            )

    # --------------------------------------------------------
    # Summary report
    # --------------------------------------------------------

    summary = {
        "validation_timestamp": now_iso(),
        "bucket": MINIO_BUCKET,
        "total_objects": len(bronze_objects),
        "passed": pass_count,
        "failed": fail_count,
        "errors": error_count,
        "files_with_warnings":
            files_with_warnings,
        "results": summary_records,
    }

    LOCAL_REPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path = (
        LOCAL_REPORT_ROOT
        / "bronze_quality_summary.json"
    )

    with open(
        summary_path,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            summary,
            f,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    # --------------------------------------------------------
    # Console summary
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("QUALITY SUMMARY")
    print("=" * 70)

    print(
        f"Total objects       : "
        f"{len(bronze_objects)}"
    )
    print(
        f"PASS                : "
        f"{pass_count}"
    )
    print(
        f"FAIL                : "
        f"{fail_count}"
    )
    print(
        f"ERROR               : "
        f"{error_count}"
    )
    print(
        f"Files with warnings : "
        f"{files_with_warnings}"
    )

    print(
        f"\nSummary report: "
        f"{summary_path}"
    )

    print("=" * 70)

    # Validation execution errors are different from data-quality FAIL.
    # Exit non-zero only for validator/system errors.
    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()