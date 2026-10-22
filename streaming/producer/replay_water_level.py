
import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import functions as F

from transformation.silver.common import (
    create_spark_session,
    stop_spark,
)

ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / "reports" / "streaming"

DEFAULT_TABLE = "lakehouse.silver.water_level"
DEFAULT_TOPIC = "hydrology.water_level.raw"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Historical Water Level Kafka Replay"
    )

    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)

    parser.add_argument("--timestamp-col", default="timestamp")
    parser.add_argument("--value-col", default="water_level_m")
    parser.add_argument("--station-col", default=None)
    parser.add_argument("--quality-col", default=None)

    parser.add_argument("--station-code", default="039803")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--rate", type=float, default=2.0)
    parser.add_argument(
        "--start-time",
        default=None,
        help="Start observation time in Asia/Ho_Chi_Minh, inclusive (YYYY-MM-DD HH:MM:SS)",
    )

    parser.add_argument(
        "--end-time",
        default=None,
        help="End observation time in Asia/Ho_Chi_Minh, exclusive (YYYY-MM-DD HH:MM:SS)",
    )

    parser.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def make_event_id(table, station, timestamp):
    # Một event ứng với một trạm và một thời điểm quan trắc.
    # Nếu nguồn có nhiều phép đo hợp lệ cùng thời điểm,
    # cần bổ sung source record ID vào khóa này.
    raw = f"{table}|{station}|{timestamp}"
    digest = hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()

    return f"wl-{digest[:24]}"



def to_iso_timestamp(value):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    if not isinstance(value, str):
        raise ValueError(
            f"Unexpected timestamp type: {type(value)}"
        )

    parsed = datetime.strptime(
        value,
        "%Y-%m-%dT%H:%M:%S"
    )

    local_dt = parsed.replace(
        tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")
    )

    return local_dt.isoformat()



def prepare_dataframe(spark, args):
    df = spark.table(args.table)
    if args.start_time:
        df = df.filter(
            F.col(args.timestamp_col)
            >= F.to_timestamp(F.lit(args.start_time))
        )

    if args.end_time:
        df = df.filter(
            F.col(args.timestamp_col)
            < F.to_timestamp(F.lit(args.end_time))
        )

    required = [
        args.timestamp_col,
        args.value_col,
        "station_code",
        "quality_flag",
    ]

    missing = [
        col for col in required
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing columns: {missing}. "
            f"Available: {df.columns}"
        )

    if args.limit <= 0:
        raise ValueError("--limit must be positive")

    if args.rate <= 0:
        raise ValueError("--rate must be positive")

    result = (
        df.select(
            F.date_format(
                F.col(args.timestamp_col),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).alias("event_timestamp"),

            F.col(args.value_col)
             .cast("double")
             .alias("water_level_m"),

            F.col("station_code")
             .cast("string")
             .alias("station_code"),

            F.col("quality_flag")
             .cast("string")
             .alias("source_quality_flag"),
        )
        .filter(
            F.col("event_timestamp").isNotNull()
            & F.col("water_level_m").isNotNull()
            & F.col("station_code").isNotNull()
        )
        .orderBy(
            "event_timestamp",
            "station_code",
        )
        .limit(args.limit)
    )

    return result



def build_event(row, table, replay_run_id):
    timestamp = to_iso_timestamp(
        row["event_timestamp"]
    )

    station = row["station_code"]

    return {
        "schema_version": "1.0",
        "event_id": make_event_id(
            table, station, timestamp
        ),
        "event_type": "water_level_observation",
        "station_code": station,
        "event_time": timestamp,
        "water_level_m": row["water_level_m"],
        "source_quality_flag": row["source_quality_flag"],
        "source_table": table,
        "simulation": True,
        "simulation_type": "historical_replay",
        "replay_run_id": replay_run_id,
        "replayed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def main():
    args = parse_args()
    spark = None
    producer = None

    started = time.time()
    sent = 0
    failed = 0
    attempted = 0

    replay_run_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    try:
        spark = create_spark_session(
            app_name="water-level-kafka-replay",
            master="local[2]",
        )

        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh",
        )
        spark.sparkContext.setLogLevel("ERROR")

        df = prepare_dataframe(spark, args)

        print("\n=== REPLAY DATA CHECK ===")
        print("Prepared rows:", df.count())
        df.printSchema()
        df.show(5, truncate=False)

        prepared_count = df.count()

        if prepared_count == 0:
            raise ValueError(
                "No valid water level records found. "
                "Check timestamp/value columns and NULL values."
            )

        if not args.dry_run:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8"),
                acks="all",
                retries=3,
                max_block_ms=10000,
            )

        print("=" * 60)
        print("S2 - WATER LEVEL HISTORICAL REPLAY")
        print("=" * 60)
        print("Source:", args.table)
        print("Topic:", args.topic)
        print("Limit:", args.limit)
        print("Rate:", args.rate, "events/second")
        print("Dry run:", args.dry_run)

        for row in df.toLocalIterator():
            event = build_event(
                row, args.table, replay_run_id
            )

            attempted += 1

            if args.dry_run:
                print(json.dumps(
                    event, ensure_ascii=False
                ))
                sent += 1
                continue

            try:
                metadata = producer.send(
                    args.topic,
                    key=event["station_code"],
                    value=event,
                ).get(timeout=15)

                sent += 1

                print(
                    f"[SENT {sent}] "
                    f"partition={metadata.partition} "
                    f"offset={metadata.offset} "
                    f"event_time={event['event_time']}"
                )

            except Exception as exc:
                failed += 1
                print(
                    f"[FAILED] {event['event_id']}: {exc}"
                )

            time.sleep(1.0 / args.rate)

    finally:
        if producer is not None:
            producer.flush(timeout=20)
            producer.close(timeout=20)

        if spark is not None:
            stop_spark(spark)

        REPORT_DIR.mkdir(
            parents=True, exist_ok=True
        )

        report = {
            "step": "S2",
            "replay_run_id": replay_run_id,
            "source_table": args.table,
            "topic": args.topic,
            "dry_run": args.dry_run,
            "attempted": attempted,
            "sent": sent,
            "failed": failed,
            "duration_seconds": round(
                time.time() - started, 2
            ),
            "status": (
                "PASS"
                if attempted == args.limit
                and failed == 0
                else "FAIL"
            ),
        }

        report_path = (
            REPORT_DIR
            / f"water_level_replay_{replay_run_id}.json"
        )

        report_path.write_text(
            json.dumps(
                report, indent=2, ensure_ascii=False
            ),
            encoding="utf-8",
        )

        print("\n" + "=" * 60)
        print("S2 REPLAY RESULT")
        print("=" * 60)
        print("STATUS:", report["status"])
        print("ATTEMPTED:", attempted)
        print("SENT:", sent)
        print("FAILED:", failed)
        print("REPORT:", report_path)


if __name__ == "__main__":
    main()
