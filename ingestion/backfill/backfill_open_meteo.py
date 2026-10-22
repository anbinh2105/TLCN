
import argparse
import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIG
# ============================================================

LATITUDE = 10.027
LONGITUDE = 105.769
TIMEZONE = "Asia/Ho_Chi_Minh"

START = pd.Timestamp("2018-04-25 14:15:00")
END = pd.Timestamp("2026-09-18 15:45:00")

FIRST_YEAR = START.year
LAST_YEAR = END.year

BASE_DIR = Path("data/raw/backfill")
REPORT_DIR = Path("reports/gold/quality")

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
FLOOD_API = "https://flood-api.open-meteo.com/v1/flood"

SOURCES = ("weather", "rainfall", "river_discharge")

WEATHER_VARIABLES = (
    "temperature_2m,"
    "relative_humidity_2m,"
    "pressure_msl,"
    "wind_speed_10m,"
    "wind_direction_10m"
)


# ============================================================
# HTTP
# ============================================================

def create_session():
    session = requests.Session()

    retry = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)

    return session


def request_data(session, source, start_date, end_date):
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": TIMEZONE,
        "cell_selection": "nearest",
    }

    if source == "weather":
        url = ARCHIVE_API
        params.update({
            "hourly": WEATHER_VARIABLES,
            "models": "era5",
            "temperature_unit": "celsius",
            "wind_speed_unit": "ms",
        })

    elif source == "rainfall":
        url = ARCHIVE_API
        params.update({
            "hourly": "rain",
            "models": "era5",
            "precipitation_unit": "mm",
        })

    else:
        url = FLOOD_API
        params.update({
            "daily": "river_discharge",
        })

    response = session.get(
        url, params=params, timeout=(15, 180)
    )

    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise RuntimeError(
            f"API error: {data.get('reason')}"
        )

    returned_timezone = data.get("timezone")

    if returned_timezone != TIMEZONE:
        raise ValueError(
            f"Unexpected timezone: {returned_timezone}"
        )

    return data, {
        "api_url": url,
        "request_params": params,
        "response_timezone": returned_timezone,
        "response_latitude": data.get("latitude"),
        "response_longitude": data.get("longitude"),
        "response_utc_offset_seconds": data.get(
            "utc_offset_seconds"
        ),
        "response_units": data.get(
            "daily_units" if source == "river_discharge"
            else "hourly_units", {}
        ),
    }


# ============================================================
# DATE WINDOWS
# ============================================================

def missing_windows():
    """
    Missing dates relative to the existing Raw files.

    Existing Raw coverage each year:
      Apr 25 through Sep 18, inclusive.

    2018 starts Apr 25.
    2026 ends Sep 18.
    """
    windows = []

    for year in range(FIRST_YEAR, LAST_YEAR + 1):

        if year > FIRST_YEAR:
            windows.append((
                date(year, 1, 1),
                date(year, 4, 24),
            ))

        if year < LAST_YEAR:
            windows.append((
                date(year, 9, 19),
                date(year, 12, 31),
            ))

    return windows


# ============================================================
# PARSE RESPONSE
# ============================================================

def build_dataframe(source, data):
    if source == "weather":
        hourly = data["hourly"]

        df = pd.DataFrame({
            "timestamp": hourly["time"],
            "temperature": hourly["temperature_2m"],
            "humidity": hourly["relative_humidity_2m"],
            "pressure": hourly["pressure_msl"],
            "wind_speed": hourly["wind_speed_10m"],
            "wind_direction": hourly["wind_direction_10m"],
        })

        df["latitude"] = data["latitude"]
        df["longitude"] = data["longitude"]

    elif source == "rainfall":
        hourly = data["hourly"]

        df = pd.DataFrame({
            "timestamp": hourly["time"],
            "rainfall_mm": hourly["rain"],
        })

        df["latitude"] = data["latitude"]
        df["longitude"] = data["longitude"]

    else:
        daily = data["daily"]

        df = pd.DataFrame({
            "timestamp": daily["time"],
            "station": "Hau River - Can Tho",
            "discharge_m3s": daily["river_discharge"],
        })

    df["timestamp"] = pd.to_datetime(
        df["timestamp"], errors="coerce"
    )

    for col in df.columns:
        if col not in ("timestamp", "station"):
            df[col] = pd.to_numeric(
                df[col], errors="coerce"
            )

    # Preserve original CSV schema.
    df["date"] = df["timestamp"].dt.date
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    if source != "river_discharge":
        df["day"] = df["timestamp"].dt.day
        df["hour"] = df["timestamp"].dt.hour

    return df


# ============================================================
# VALIDATION
# ============================================================

def validate(df, source, start_date, end_date):
    frequency = (
        "D" if source == "river_discharge" else "h"
    )

    expected = pd.date_range(
        start=pd.Timestamp(start_date),
        end=(
            pd.Timestamp(end_date)
            if frequency == "D"
            else pd.Timestamp(end_date) + pd.Timedelta(hours=23)
        ),
        freq=frequency,
    )

    actual = pd.DatetimeIndex(df["timestamp"])

    if actual.hasnans:
        raise ValueError("Null or invalid timestamp")

    if actual.has_duplicates:
        raise ValueError("Duplicate timestamps")

    if len(actual) != len(expected):
        raise ValueError(
            f"Row count mismatch: "
            f"actual={len(actual)}, expected={len(expected)}"
        )

    if not actual.sort_values().equals(expected):
        raise ValueError(
            "Timestamp coverage is not continuous or "
            "does not match the requested interval"
        )

    measurement_columns = {
        "weather": [
            "temperature", "humidity", "pressure",
            "wind_speed", "wind_direction"
        ],
        "rainfall": ["rainfall_mm"],
        "river_discharge": ["discharge_m3s"],
    }[source]

    null_counts = {
        col: int(df[col].isna().sum())
        for col in measurement_columns
    }

    return {
        "status": "PASS_TIMESTAMP_COVERAGE",
        "row_count": len(df),
        "start": str(actual.min()),
        "end": str(actual.max()),
        "null_counts": null_counts,
        "measurement_values_complete": all(
            count == 0 for count in null_counts.values()
        ),
    }


# ============================================================
# SAVE
# ============================================================

def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(
            lambda: file.read(1024 * 1024), b""
        ):
            digest.update(block)

    return digest.hexdigest()


def save_new_file(df, path):
    """
    Atomic, no-overwrite file creation.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        raise FileExistsError(path)

    temporary = path.with_name(
        path.name + f".{os.getpid()}.tmp"
    )

    try:
        df.to_csv(
            temporary,
            index=False,
            encoding="utf-8-sig",
        )

        # Exclusive final creation: never replace an existing file.
        with temporary.open("rb") as src:
            with path.open("xb") as dst:
                for block in iter(
                    lambda: src.read(1024 * 1024), b""
                ):
                    dst.write(block)

    finally:
        temporary.unlink(missing_ok=True)


def process_window(
    session, source, start_date, end_date, dry_run
):
    filename = (
        f"{source}_backfill_"
        f"{start_date:%Y%m%d}_{end_date:%Y%m%d}.csv"
    )

    output = BASE_DIR / source / filename

    print(
        f"\n[{source.upper()}] "
        f"{start_date} -> {end_date}"
    )

    if dry_run:
        print("DRY RUN:", output)
        return {"status": "PLANNED", "file": str(output)}

    if output.exists():
        print("Existing file: validating before skip")

        existing = pd.read_csv(output)
        existing["timestamp"] = pd.to_datetime(
            existing["timestamp"], errors="coerce"
        )

        check = validate(
            existing, source, start_date, end_date
        )

        print("SKIP: existing file passed timestamp checks")

        return {
            "status": "SKIPPED_VERIFIED",
            "file": str(output),
            "sha256": sha256(output),
            "validation": check,
        }

    data, api_metadata = request_data(
        session, source, start_date, end_date
    )

    df = build_dataframe(source, data)

    check = validate(
        df, source, start_date, end_date
    )

    # Do not silently remove null measurements.
    if not check["measurement_values_complete"]:
        print("WARNING: null measurements detected")
        print(check["null_counts"])

    save_new_file(df, output)

    result = {
        "status": "DOWNLOADED",
        "source": source,
        "file": str(output),
        "sha256": sha256(output),
        "validation": check,
        "api_metadata": api_metadata,
        "downloaded_at_utc": datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat(),
    }

    metadata_path = output.with_suffix(".metadata.json")

    with metadata_path.open("x", encoding="utf-8") as f:
        json.dump(
            result, f, indent=2, ensure_ascii=False,
            default=str
        )

    print(
        f"PASS: {check['row_count']} rows | "
        f"{output.name}"
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        choices=(*SOURCES, "all"),
        default="all",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    selected = (
        SOURCES if args.source == "all"
        else (args.source,)
    )

    windows = missing_windows()

    print("=" * 65)
    print("OPEN-METEO MISSING-DATE BACKFILL")
    print("=" * 65)
    print("Target Water Level range:", START, "->", END)
    print("Sources:", ", ".join(selected))
    print("Missing date windows:", len(windows))
    print("Dry run:", args.dry_run)

    session = create_session()

    results = []
    failed = 0

    for source in selected:
        for start_date, end_date in windows:
            try:
                result = process_window(
                    session,
                    source,
                    start_date,
                    end_date,
                    args.dry_run,
                )
                results.append(result)

                if not args.dry_run:
                    time.sleep(1)

            except Exception as exc:
                failed += 1

                print("FAIL:", source, start_date, exc)

                results.append({
                    "status": "FAILED",
                    "source": source,
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                    "error": str(exc),
                })

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    report_path = REPORT_DIR / (
        "open_meteo_backfill_plan.json"
        if args.dry_run
        else "open_meteo_backfill_result.json"
    )

    # Do not overwrite an existing report.
    if report_path.exists():
        report_path = REPORT_DIR / (
            f"{report_path.stem}_"
            f"{datetime.now():%Y%m%d_%H%M%S_%f}.json"
        )

    with report_path.open("x", encoding="utf-8") as f:
        json.dump(
            {
                "target_start": str(START),
                "target_end": str(END),
                "dry_run": args.dry_run,
                "failed_windows": failed,
                "results": results,
            },
            f, indent=2, ensure_ascii=False
        )

    session.close()

    print("\n" + "=" * 65)
    print("Total windows:", len(results))
    print("Failed:", failed)
    print("Report:", report_path)

    if failed:
        print("BACKFILL: INCOMPLETE")
        raise SystemExit(1)

    print(
        "BACKFILL PLAN: PASS"
        if args.dry_run
        else "BACKFILL DOWNLOAD: PASS"
    )


if __name__ == "__main__":
    main()
