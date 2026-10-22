
import math
from pyspark.sql import functions as F


def result(rule_id, table, passed, observed, message, severity="CRITICAL"):
    status = "PASS" if passed else (
        "FAIL" if severity == "CRITICAL" else "WARN"
    )
    return {
        "rule_id": rule_id,
        "table": table,
        "status": status,
        "severity": severity,
        "observed": observed,
        "message": message,
    }


def check_schema(df, table, required, rule_id):
    actual = dict(df.dtypes)
    mismatches = {
        name: {"expected": dtype, "actual": actual.get(name)}
        for name, dtype in required.items()
        if actual.get(name) != dtype
    }
    return result(
        rule_id, table, not mismatches, mismatches,
        "Required columns and types"
    )


def check_not_null(df, table, columns, rule_id):
    condition = None
    for name in columns:
        expr = F.col(name).isNull()
        condition = expr if condition is None else condition | expr
    bad = df.filter(condition).count()
    return result(
        rule_id, table, bad == 0, bad,
        "Rows with NULL in required columns"
    )


def check_unique(df, table, keys, rule_id):
    bad = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    return result(
        rule_id, table, bad == 0, bad,
        "Duplicate logical key groups"
    )


def check_finite(df, table, columns, rule_id):
    condition = None
    for name in columns:
        value = F.col(name).cast("double")
        expr = (
            value.isNull()
            | F.isnan(value)
            | (F.abs(value) == F.lit(float("inf")))
        )
        condition = expr if condition is None else condition | expr
    bad = df.filter(condition).count()
    return result(
        rule_id, table, bad == 0, bad,
        "Rows with NULL, NaN or infinite numeric values"
    )


def check_range_order(df, table, low, mean, high, rule_id):
    bad = df.filter(
        F.col(low).isNull()
        | F.col(mean).isNull()
        | F.col(high).isNull()
        | (F.col(low) > F.col(mean))
        | (F.col(mean) > F.col(high))
    ).count()
    return result(
        rule_id, table, bad == 0, bad,
        "Min <= Mean <= Max"
    )


def check_allowed(df, table, column, values, rule_id):
    bad = df.filter(
        F.col(column).isNull()
        | ~F.col(column).isin(*values)
    ).count()
    return result(
        rule_id, table, bad == 0, bad,
        f"Allowed values for {column}: {values}"
    )


def check_condition(df, table, condition, rule_id, message):
    bad = df.filter(~F.coalesce(condition, F.lit(False))).count()
    return result(rule_id, table, bad == 0, bad, message)


def check_warning_count(df, table, condition, rule_id, message):
    count = df.filter(condition).count()
    return result(
        rule_id, table, count == 0, count,
        message, severity="WARNING"
    )


def check_equal_datasets(
    left, right, keys, values, table, rule_id, tolerance=1e-9
):
    """
    Compare two datasets by unique keys.
    Null-safe for strings/booleans and tolerant for numeric values.
    Both sides must already have unique keys.
    """
    a, b = left.alias("a"), right.alias("b")
    join = None

    for key in keys:
        expr = F.col(f"a.{key}") == F.col(f"b.{key}")
        join = expr if join is None else join & expr

    paired = a.join(b, join, "full_outer")
    bad_condition = None

    for key in keys:
        expr = (
            F.col(f"a.{key}").isNull()
            | F.col(f"b.{key}").isNull()
        )
        bad_condition = (
            expr if bad_condition is None else bad_condition | expr
        )

    left_types = dict(left.dtypes)
    right_types = dict(right.dtypes)

    numeric_types = {
        "double", "float", "int", "bigint", "smallint", "tinyint", "long"
    }

    for left_col, right_col in values:
        x = F.col(f"a.{left_col}")
        y = F.col(f"b.{right_col}")

        if (
            left_types[left_col] in numeric_types
            and right_types[right_col] in numeric_types
        ):
            same = (
                (x.isNull() & y.isNull())
                | (
                    x.isNotNull()
                    & y.isNotNull()
                    & ~F.isnan(x.cast("double"))
                    & ~F.isnan(y.cast("double"))
                    & (
                        F.abs(x.cast("double") - y.cast("double"))
                        <= tolerance
                    )
                )
            )
        else:
            same = x.eqNullSafe(y)

        bad_condition = bad_condition | ~same

    mismatches = paired.filter(bad_condition).count()

    return result(
        rule_id, table, mismatches == 0, mismatches,
        "Cross-layer consistency"
    )
