
import argparse
import csv
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKFILL_ROOT = PROJECT_ROOT / "data" / "raw" / "backfill"
LOG_PATH = PROJECT_ROOT / "data" / "metadata" / "ingestion_log.jsonl"
REPORT_DIR = PROJECT_ROOT / "reports" / "gold" / "quality"

SOURCES = ("weather", "rainfall", "river_discharge")
TIMEZONE = "Asia/Ho_Chi_Minh"

load_dotenv(PROJECT_ROOT / ".env")

BUCKET = os.getenv("MINIO_BUCKET", "flood-lakehouse")
ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def count_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.reader(stream)
        next(reader, None)
        return sum(1 for row in reader if any(
            cell.strip() for cell in row
        ))


def expected_windows():
    result = []

    for year in range(2018, 2027):
        if year > 2018:
            result.append(
                (f"{year}0101", f"{year}0424")
            )

        if year < 2026:
            result.append(
                (f"{year}0919", f"{year}1231")
            )

    return result


def expected_files():
    files = []

    for source in SOURCES:
        for start, end in expected_windows():
            filename = (
                f"{source}_backfill_{start}_{end}.csv"
            )
            files.append((
                source,
                BACKFILL_ROOT / source / filename,
            ))

    return files


def load_successful_records():
    records = {}

    if not LOG_PATH.exists():
        return records

    with LOG_PATH.open(
        "r", encoding="utf-8"
    ) as stream:
        for line in stream:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            if (
                item.get("status") == "SUCCESS"
                and item.get("checksum")
                and item.get("object_path")
            ):
                key = (
                    item.get("source"),
                    item["checksum"],
                )
                records.setdefault(key, []).append(item)

    return records


def create_client():
    if not ACCESS_KEY or not SECRET_KEY:
        raise RuntimeError(
            "Missing MINIO_ACCESS_KEY / MINIO_SECRET_KEY"
        )

    return boto3.client(
        "s3",
        endpoint_url=f"http://{ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3},
        ),
    )


def object_exists(client, key):
    try:
        response = client.head_object(
            Bucket=BUCKET, Key=key
        )
        return True, response

    except ClientError as exc:
        code = exc.response["Error"].get("Code")

        if code in ("404", "NoSuchKey", "NotFound"):
            return False, None

        raise


def validate_download_metadata(source, path):
    metadata_path = path.with_suffix(".metadata.json")

    if not path.is_file():
        raise RuntimeError(f"Missing CSV: {path}")

    if not metadata_path.is_file():
        raise RuntimeError(
            f"Missing metadata: {metadata_path}"
        )

    with metadata_path.open(
        "r", encoding="utf-8"
    ) as stream:
        metadata = json.load(stream)

    actual_hash = sha256_file(path)

    if metadata.get("sha256") != actual_hash:
        raise RuntimeError(
            f"Checksum mismatch: {path.name}"
        )

    if metadata.get("source") != source:
        raise RuntimeError(
            f"Source mismatch: {path.name}"
        )

    if metadata.get("status") != "DOWNLOADED":
        raise RuntimeError(
            f"Unexpected download status: {path.name}"
        )

    validation = metadata.get("validation", {})

    if validation.get("status") != "PASS_TIMESTAMP_COVERAGE":
        raise RuntimeError(
            f"Timestamp validation missing: {path.name}"
        )

    api = metadata.get("api_metadata", {})

    if api.get("response_timezone") != TIMEZONE:
        raise RuntimeError(
            f"Unexpected timezone: {path.name}"
        )

    row_count = count_csv(path)

    if row_count != validation.get("row_count"):
        raise RuntimeError(
            f"Row count mismatch: {path.name}"
        )

    if row_count <= 0:
        raise RuntimeError(f"Empty CSV: {path.name}")

    return actual_hash, row_count


def append_log(record):
    LOG_PATH.parent.mkdir(
        parents=True, exist_ok=True
    )

    with LOG_PATH.open(
        "a", encoding="utf-8"
    ) as stream:
        stream.write(
            json.dumps(record, ensure_ascii=False)
            + "\n"
        )
        stream.flush()
        os.fsync(stream.fileno())


def make_record(
    source, path, checksum, count,
    batch_id, key, status, note=None
):
    return {
        "batch_id": batch_id,
        "source": source,
        "source_file": path.name,
        "object_path": key,
        "ingestion_timestamp": datetime.now(
            ZoneInfo(TIMEZONE)
        ).isoformat(),
        "file_size": path.stat().st_size,
        "checksum": checksum,
        "record_count": count,
        "status": status,
        "ingestion_type": "BACKFILL",
        "note": note,
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run", action="store_true"
    )

    args = parser.parse_args()

    now = datetime.now(ZoneInfo(TIMEZONE))
    batch_id = now.strftime(
        "%Y%m%d_%H%M%S_backfill"
    )
    ingestion_date = now.date().isoformat()

    print("=" * 70)
    print("BRONZE INCREMENTAL BACKFILL")
    print("=" * 70)
    print("Bucket:", BUCKET)
    print("Batch ID:", batch_id)
    print("Dry run:", args.dry_run)

    client = create_client()
    client.head_bucket(Bucket=BUCKET)

    known = load_successful_records()
    planned = []
    errors = []

    # ========================================================
    # PREFLIGHT: validate ALL files before any upload
    # ========================================================

    for source, path in expected_files():
        try:
            checksum, count = validate_download_metadata(
                source, path
            )

            key = (
                f"bronze/{source}/"
                f"ingestion_date={ingestion_date}/"
                f"batch_id={batch_id}/"
                f"{path.name}"
            )

            # Verify old SUCCESS entries against MinIO.
            verified_existing = None

            for old in known.get(
                (source, checksum), []
            ):
                old_key = old["object_path"]

                exists, info = object_exists(
                    client, old_key
                )

                if exists:
                    metadata = info.get("Metadata", {})
                    remote_hash = metadata.get("sha256")

                    # Older objects may lack sha256 metadata.
                    # Do not blindly trust an unverified object.
                    if remote_hash == checksum:
                        verified_existing = old_key
                        break

                    errors.append(
                        f"Existing object cannot be verified "
                        f"by metadata: {old_key}"
                    )

            planned.append({
                "source": source,
                "path": path,
                "checksum": checksum,
                "record_count": count,
                "object_key": key,
                "verified_existing": verified_existing,
            })

        except Exception as exc:
            errors.append(f"{path}: {exc}")

    print("\nPreflight files:", len(planned))
    print("Preflight errors:", len(errors))

    if errors:
        for error in errors:
            print("[ERROR]", error)

        print("PREFLIGHT: FAIL — no uploads performed")
        raise SystemExit(1)

    if len(planned) != 48:
        raise RuntimeError(
            f"Expected 48 files, found {len(planned)}"
        )

    print("PREFLIGHT: PASS")

    if args.dry_run:
        for item in planned:
            status = (
                "SKIP_VERIFIED"
                if item["verified_existing"]
                else "UPLOAD"
            )
            print(
                status,
                item["source"],
                item["path"].name,
                item["record_count"],
            )

        print("DRY RUN: PASS — no uploads performed")
        return

    # ========================================================
    # UPLOAD: conditional no-overwrite
    # ========================================================

    summary = {
        "SUCCESS": 0,
        "SKIPPED": 0,
        "FAILED": 0,
    }

    for item in planned:
        source = item["source"]
        path = item["path"]
        checksum = item["checksum"]
        count = item["record_count"]
        key = item["object_key"]

        if item["verified_existing"]:
            print(
                "[SKIPPED] Already ingested:",
                item["verified_existing"]
            )
            summary["SKIPPED"] += 1
            continue

        # Recheck file immediately before upload.
        if sha256_file(path) != checksum:
            print("[FAILED] Local file changed:", path)
            summary["FAILED"] += 1
            continue

        try:
            with path.open("rb") as stream:
                client.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=stream,
                    ContentLength=path.stat().st_size,
                    ContentType="text/csv",
                    IfNoneMatch="*",
                    Metadata={
                        "sha256": checksum,
                        "source": source,
                        "ingestion-type": "backfill",
                    },
                )

            exists, info = object_exists(client, key)

            if not exists:
                raise RuntimeError(
                    "Object missing after upload"
                )

            if info.get("Metadata", {}).get("sha256") != checksum:
                raise RuntimeError(
                    "Remote checksum metadata mismatch"
                )

            record = make_record(
                source, path, checksum, count,
                batch_id, key, "SUCCESS"
            )
            append_log(record)

            summary["SUCCESS"] += 1
            print(
                "[SUCCESS]",
                source,
                path.name,
                f"rows={count}"
            )

        except Exception as exc:
            summary["FAILED"] += 1
            print(
                "[FAILED]",
                source,
                path.name,
                str(exc),
            )

            record = make_record(
                source, path, checksum, count,
                batch_id, key, "FAILED",
                note=str(exc),
            )
            append_log(record)

    print("\n" + "=" * 70)
    print("BRONZE BACKFILL SUMMARY")
    print("=" * 70)

    for status, count in summary.items():
        print(f"{status}: {count}")

    REPORT_DIR.mkdir(
        parents=True, exist_ok=True
    )

    report_path = REPORT_DIR / (
        f"bronze_backfill_{batch_id}.json"
    )

    with report_path.open(
        "x", encoding="utf-8"
    ) as stream:
        json.dump({
            "batch_id": batch_id,
            "ingestion_date": ingestion_date,
            "bucket": BUCKET,
            "summary": summary,
        }, stream, indent=2)

    print("Report:", report_path)

    if summary["FAILED"]:
        raise SystemExit(1)

    print("BRONZE BACKFILL: PASS")


if __name__ == "__main__":
    main()
