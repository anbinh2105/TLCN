
import csv
import json
from pathlib import Path

import pandas as pd


SOURCES = {
    "weather": Path("data/raw/weather"),
    "rainfall": Path("data/raw/rainfall"),
    "river_discharge": Path("data/raw/river_discharge"),
}

OUTPUT = Path(
    "reports/gold/quality/raw_coverage_investigation.json"
)

TIMESTAMP_CANDIDATES = (
    "timestamp",
    "datetime",
    "date_time",
    "observation_time",
    "date",
    "ngay",
    "ngày",
)


def detect_separator(path):
    with path.open(
        "r", encoding="utf-8-sig", errors="replace"
    ) as f:
        sample = f.read(8192)

    try:
        return csv.Sniffer().sniff(
            sample, delimiters=",;\t"
        ).delimiter
    except csv.Error:
        return ";" if sample.count(";") > sample.count(",") else ","


def find_timestamp_column(columns):
    normalized = {
        str(col).strip().lower(): col
        for col in columns
    }

    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in normalized:
            return normalized[candidate]

    return None


def parse_timestamps(series):
    """
    Try common date formats, then fall back to Pandas.

    Dates are treated as local source timestamps.
    No timezone conversion is performed.
    """
    values = series.astype("string").str.strip()

    result = pd.Series(
        pd.NaT, index=series.index, dtype="datetime64[ns]"
    )

    formats = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ]

    for fmt in formats:
        mask = result.isna() & values.notna()

        if not mask.any():
            break

        parsed = pd.to_datetime(
            values.loc[mask],
            format=fmt,
            errors="coerce"
        )

        result.loc[mask] = parsed

    mask = result.isna() & values.notna()

    if mask.any():
        result.loc[mask] = pd.to_datetime(
            values.loc[mask],
            errors="coerce",
            dayfirst=True
        )

    return result


def inspect_file(path):
    separator = detect_separator(path)

    df = pd.read_csv(
        path,
        sep=separator,
        encoding="utf-8-sig",
        low_memory=False
    )

    ts_col = find_timestamp_column(df.columns)

    result = {
        "file": str(path),
        "separator": separator,
        "rows": len(df),
        "columns": list(map(str, df.columns)),
        "timestamp_column": ts_col,
    }

    if ts_col is None:
        result["error"] = "TIMESTAMP_COLUMN_NOT_FOUND"
        return result, pd.Series(dtype="datetime64[ns]")

    timestamps = parse_timestamps(df[ts_col])

    result["invalid_timestamps"] = int(
        timestamps.isna().sum()
    )

    valid = timestamps.dropna()

    if valid.empty:
        result["error"] = "NO_VALID_TIMESTAMPS"
        return result, valid

    result["min_timestamp"] = str(valid.min())
    result["max_timestamp"] = str(valid.max())

    return result, valid


def main():
    report = {
        "analysis": "raw_coverage_investigation",
        "sources": {},
    }

    for source, folder in SOURCES.items():
        print("\n" + "=" * 65)
        print("RAW SOURCE:", source.upper())
        print("=" * 65)

        files = sorted(folder.rglob("*.csv"))

        source_report = {
            "file_count": len(files),
            "files": [],
            "yearly_coverage": [],
            "status": "NOT_CHECKED",
        }

        all_dates = set()
        all_hours = set()
        total_rows = 0
        errors = 0

        for path in files:
            try:
                info, timestamps = inspect_file(path)
                source_report["files"].append(info)

                total_rows += info["rows"]

                if "error" in info:
                    errors += 1
                    print("WARNING:", path, info["error"])
                    continue

                all_dates.update(
                    timestamps.dt.date.tolist()
                )

                if source != "river_discharge":
                    all_hours.update(
                        timestamps.dt.floor("h").tolist()
                    )

                print(
                    path.name,
                    "| rows:", info["rows"],
                    "| from:", info["min_timestamp"],
                    "| to:", info["max_timestamp"],
                    "| invalid:", info["invalid_timestamps"]
                )

            except Exception as exc:
                errors += 1

                source_report["files"].append({
                    "file": str(path),
                    "error": str(exc),
                })

                print("ERROR:", path, exc)

        source_report["total_rows"] = total_rows
        source_report["unique_dates"] = len(all_dates)
        source_report["unique_hours"] = len(all_hours)
        source_report["file_errors"] = errors

        years = sorted({d.year for d in all_dates})

        for year in years:
            dates = sorted(
                d for d in all_dates if d.year == year
            )

            outside = [
                d for d in dates
                if not (
                    (4, 25) <= (d.month, d.day)
                    <= (9, 18)
                )
            ]

            hours_in_year = sum(
                1 for ts in all_hours if ts.year == year
            )

            entry = {
                "year": year,
                "unique_days": len(dates),
                "unique_hours": hours_in_year,
                "first_date": str(dates[0]),
                "last_date": str(dates[-1]),
                "days_outside_25Apr_18Sep": len(outside),
            }

            source_report["yearly_coverage"].append(entry)

            print(
                f"{year}: {len(dates)} days, "
                f"{hours_in_year} hours, "
                f"outside seasonal window: {len(outside)}"
            )

        if not files:
            source_report["status"] = "NO_FILES"
        elif errors:
            source_report["status"] = "INCOMPLETE_INSPECTION"
        else:
            source_report["status"] = "INSPECTED"

        report["sources"][source] = source_report

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT.open("w", encoding="utf-8") as f:
        json.dump(
            report, f, ensure_ascii=False, indent=2
        )

    print("\nRAW COVERAGE INVESTIGATION: COMPLETED")
    print("Report:", OUTPUT)


if __name__ == "__main__":
    main()
