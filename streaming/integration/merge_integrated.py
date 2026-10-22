
import argparse
import tempfile

from pyspark.sql import functions as F

from transformation.silver.common import create_spark_session
from streaming.integration.build_integrated import (
    STREAM_TABLE,
    BATCH_TABLE,
    build_integrated,
    validate_unique,
)


TARGET_TABLE = "lakehouse.gold.hourly_hydrology_stream"

KEYS = ["hour_start", "location_id", "station_code"]

SOURCE_VIEW = "s6_integrated_source"


def create_target(spark, df):
    """
    Create an empty Iceberg table only if it does not exist.
    Never drop or overwrite an existing table.
    """
    if spark.catalog.tableExists(TARGET_TABLE):
        print("TARGET TABLE: EXISTS")
        return

    definitions = [
        f"`{field.name}` {field.dataType.simpleString()}"
        for field in df.schema.fields
    ]

    definitions.append("`gold_processed_at` TIMESTAMP")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            {", ".join(definitions)}
        )
        USING iceberg
        """
    )

    print("TARGET TABLE: CREATED")


def validate_target_schema(spark, source):
    target = spark.table(TARGET_TABLE)

    expected = {
        field.name: field.dataType.simpleString()
        for field in source.schema.fields
    }

    expected["gold_processed_at"] = "timestamp"

    actual = {
        field.name: field.dataType.simpleString()
        for field in target.schema.fields
    }

    if expected != actual:
        raise RuntimeError(
            "Target schema mismatch.\n"
            f"Expected: {expected}\n"
            f"Actual: {actual}"
        )


def validate_target_keys(spark):
    target = spark.table(TARGET_TABLE)

    validate_unique(
        target,
        KEYS,
        "TARGET"
    )


def merge_to_iceberg(spark, source):
    source.createOrReplaceTempView(SOURCE_VIEW)

    columns = source.columns

    join_condition = " AND ".join(
        f"t.`{key}` = s.`{key}`"
        for key in KEYS
    )

    non_key_columns = [
        column
        for column in columns
        if column not in KEYS
    ]

    change_condition = " OR ".join(
        f"NOT (t.`{column}` <=> s.`{column}`)"
        for column in non_key_columns
    )

    update_assignments = [
        f"t.`{column}` = s.`{column}`"
        for column in non_key_columns
    ]

    update_assignments.append(
        "t.gold_processed_at = current_timestamp()"
    )

    insert_columns = columns + ["gold_processed_at"]

    insert_values = [
        f"s.`{column}`"
        for column in columns
    ] + ["current_timestamp()"]

    sql = f"""
        MERGE INTO {TARGET_TABLE} t
        USING {SOURCE_VIEW} s
        ON {join_condition}

        WHEN MATCHED AND ({change_condition})
        THEN UPDATE SET
            {", ".join(update_assignments)}

        WHEN NOT MATCHED
        THEN INSERT (
            {", ".join(f"`{c}`" for c in insert_columns)}
        )
        VALUES (
            {", ".join(insert_values)}
        )
    """

    spark.sql(sql)


def verify_result(spark, source):
    target = spark.table(TARGET_TABLE)

    join_condition = [
        F.col(f"s.{key}") == F.col(f"t.{key}")
        for key in KEYS
    ]

    condition = join_condition[0]

    for expression in join_condition[1:]:
        condition = condition & expression

    matched = (
        source.alias("s")
        .join(target.alias("t"), condition, "left")
    )

    missing = matched.filter(
        F.col("t.hour_start").isNull()
    ).count()

    if missing:
        raise RuntimeError(
            f"Missing target rows: {missing}"
        )

    value_columns = [
        column
        for column in source.columns
        if column not in KEYS
    ]

    differences = [
        ~F.col(f"s.{column}").eqNullSafe(
            F.col(f"t.{column}")
        )
        for column in value_columns
    ]

    mismatch_condition = differences[0]

    for expression in differences[1:]:
        mismatch_condition = (
            mismatch_condition | expression
        )

    mismatches = matched.filter(
        mismatch_condition
    ).count()

    source_count = source.count()
    verified_count = matched.count()
    target_count = target.count()

    if mismatches or verified_count != source_count:
        raise RuntimeError(
            "Verification failed: "
            f"mismatches={mismatches}, "
            f"verified={verified_count}, "
            f"source={source_count}"
        )

    print("\n=== S6 MERGE RESULT ===")
    print("STATUS: PASS")
    print("TABLE:", TARGET_TABLE)
    print("VERIFIED ROWS:", verified_count)
    print("VALUE MISMATCHES:", mismatches)
    print("TOTAL TABLE ROWS:", target_count)

    target.select(
        "hour_start",
        "location_id",
        "station_code",
        "water_level_mean",
        "water_level_quality_flag",
        "rainfall_mm",
        "temperature_c",
        "discharge_m3s",
        "tide_height_m",
        "present_source_count",
        "usable_source_count",
        "water_level_origin",
        "simulation",
        "gold_processed_at"
    ).orderBy(
        "hour_start",
        "location_id",
        "station_code"
    ).show(20, truncate=False)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true"
    )

    args = parser.parse_args()

    spark = create_spark_session(
        "S6-Gold-Integrated-Merge"
    )

    source = None

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "Asia/Ho_Chi_Minh"
        )

        print("\n=== S6 SOURCE CHECK ===")

        stream = spark.table(STREAM_TABLE)
        batch = spark.table(BATCH_TABLE)

        validate_unique(
            stream,
            ["hour_timestamp", "station_code"],
            "STREAM"
        )

        validate_unique(
            batch,
            ["hour_start", "location_id"],
            "BATCH"
        )

        print("STREAM ROWS:", stream.count())

        print("\n=== S6 BUILD INTEGRATED ===")

        checkpoint_dir = tempfile.mkdtemp(
            prefix="tlcn_s6_checkpoint_"
        )

        spark.sparkContext.setCheckpointDir(
            checkpoint_dir
        )

        source = (
            build_integrated(stream, batch)
            .checkpoint(eager=True)
            .cache()
        )

        source_count = source.count()

        print("INTEGRATED ROWS:", source_count)

        if source_count == 0:
            raise RuntimeError(
                "Empty integrated source"
            )

        if source_count != stream.count():
            raise RuntimeError(
                "Integration changed row count"
            )

        validate_unique(
            source,
            KEYS,
            "INTEGRATED"
        )

        if args.dry_run:
            print("\n=== S6 DRY RUN ===")
            source.orderBy("hour_start").show(
                20,
                truncate=False
            )
            print("STATUS: DRY_RUN_PASS")
            print("NO ICEBERG WRITE")
            return

        print("\n=== S6 ICEBERG MERGE ===")

        create_target(spark, source)

        validate_target_schema(spark, source)
        validate_target_keys(spark)

        merge_to_iceberg(spark, source)

        validate_target_keys(spark)
        verify_result(spark, source)

    finally:
        if source is not None:
            source.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
