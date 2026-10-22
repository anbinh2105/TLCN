
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REPORT_DIR = Path("reports/streaming_demo")

COMMANDS = [
    {
        "name": "streaming_data_quality",
        "command": [
            sys.executable,
            "-m",
            "streaming.quality.validate_streaming",
            "--demo-baseline",
        ],
    },
    {
        "name": "streaming_lineage",
        "command": [
            sys.executable,
            "-m",
            "streaming.quality.validate_lineage",
            "--demo-baseline",
        ],
    },
]


def run_validation(item):
    print(
        f"\n=== RUNNING {item['name'].upper()} ===",
        flush=True,
    )

    process = subprocess.run(
        item["command"],
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    print(process.stdout, flush=True)

    return {
        "name": item["name"],
        "command": item["command"],
        "return_code": process.returncode,
        "status": (
            "PASS"
            if process.returncode == 0
            else "FAIL"
        ),
        "output": process.stdout,
    }


def main():
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("\n=== S8.2 VALIDATION GATE ===")

    results = []

    for item in COMMANDS:
        result = run_validation(item)
        results.append(result)

        print(
            f"{item['name']}: {result['status']}",
            flush=True,
        )

        if result["status"] == "FAIL":
            print(
                "VALIDATION FAILED — STOPPING GATE",
                flush=True,
            )
            break

    passed = sum(
        result["status"] == "PASS"
        for result in results
    )

    failed = sum(
        result["status"] == "FAIL"
        for result in results
    )

    overall = (
        "PASS"
        if passed == len(COMMANDS) and failed == 0
        else "FAIL"
    )

    report = {
        "stage": "S8.2",
        "demo_type": "historical_replay",
        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "overall_status": overall,
        "summary": {
            "pass": passed,
            "fail": failed,
            "expected_checks": len(COMMANDS),
        },
        "validations": results,
    }

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%S%fZ")

    output_path = (
        REPORT_DIR
        / f"validation_gate_{timestamp}.json"
    )

    output_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\n=== S8.2 RESULT ===")
    print("STATUS:", overall)
    print("PASS:", passed)
    print("FAIL:", failed)
    print("REPORT:", output_path)
    print("NO DATA WRITTEN")

    if overall != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
