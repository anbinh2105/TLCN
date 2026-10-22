
import json
from pathlib import Path

import great_expectations as gx
import pandas as pd


REPORT_PATH = Path(
    "reports/gold/quality/gold_alignment_report.json"
)

GE_REPORT_PATH = Path(
    "reports/gold/quality/gold_ge_report.json"
)


def main():
    with REPORT_PATH.open(
        "r", encoding="utf-8"
    ) as f:
        report = json.load(f)

    records = []

    for table in report["results"]:
        for check in table["checks"]:
            records.append({
                "table": table["table"],
                "check_name": check["name"],
                "passed": int(check["success"]),
            })

    if not records:
        raise RuntimeError("No validation results")

    df = pd.DataFrame(records)

    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_pandas(
        "gold_quality_datasource"
    )

    asset = datasource.add_dataframe_asset(
        name="gold_quality_results"
    )

    batch_definition = (
        asset.add_batch_definition_whole_dataframe(
            "gold_quality_batch"
        )
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    expectation = (
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passed",
            min_value=1,
            max_value=1,
        )
    )

    result = batch.validate(expectation)

    GE_REPORT_PATH.parent.mkdir(
        parents=True, exist_ok=True
    )

    output = {
        "success": bool(result.success),
        "total_checks": len(df),
        "failed_checks": int(
            (df["passed"] == 0).sum()
        ),
    }

    with GE_REPORT_PATH.open(
        "w", encoding="utf-8"
    ) as f:
        json.dump(output, f, indent=2)

    print("GE validation:", output)

    if not result.success:
        raise RuntimeError(
            "Great Expectations validation FAILED"
        )

    print("GOLD GREAT EXPECTATIONS: PASS")


if __name__ == "__main__":
    main()
