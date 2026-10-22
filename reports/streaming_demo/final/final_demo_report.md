# S8.5 — Streaming Replay Demo: Final Validation Report

- Generated (UTC): 2026-10-13T08:17:36.213409+00:00
- Overall automated evidence status: **PASS**
- Scope: Historical replay Kafka → MinIO Bronze → Iceberg Silver → Gold Hourly → Gold Integrated
- Batch Gold: `lakehouse.gold.hourly_hydrology` (read-only)

## Evidence inventory

| Stage | Status | Source file |
|---|---|---|
| S8.1 Preflight | **PASS** | `reports\streaming_demo\preflight_20261012T065514Z.json` |
| S8.2 Validation Gate | **PASS** | `reports\streaming_demo\validation_gate_20261012T065748124664Z.json` |
| S8.3 Idempotency Precheck | **PASS** | `reports\streaming_demo\idempotency_precheck_20261012T070203819955Z.json` |
| S8.3 Actual Rerun | **PASS** | `reports\streaming_demo\rerun_20261012T104552795795Z.json` |
| S8.4 Batch Protection | **PASS** | `reports\streaming_demo\batch_protection_20261012T174843Z.json` |
| S7 Data Quality | **PASS** | `reports\streaming_quality\streaming_dq_20261012T105407Z.json` |
| S7.6 Data Lineage | **PASS** | `reports\streaming_quality\streaming_lineage_20261012T105553219985Z.json` |

## Batch Gold protection

- Expected rows: **73,656**
- Business key: `hour_start`, `location_id`
- Expected Iceberg snapshot: `5232994262495386058`
- Verified S8.4 status: **PASS**

## Idempotency interpretation

- S8.3 rerun supports **logical idempotency** (business content unchanged).
- Gold Integrated Iceberg snapshot changed in the rerun; **physical no-op is not proven**.
- Batch Gold snapshot remained unchanged.

## Limitations and warnings

- Historical Kafka replay is a simulation, not live sensor ingestion.
- S8.3 demonstrates logical idempotency; Gold Integrated snapshot changed on rerun, so physical no-op is not established.
- S8.4 unchanged Iceberg snapshot supports Batch protection; absent replay-marker columns are not independent contamination checks.
- Two partial water-level hours and three missing-tide hours are expected data-quality warnings.
- Spark Windows temporary JAR cleanup warnings do not invalidate completed validations.

## Next step

If any evidence row is REVIEW or MISSING, inspect the source JSON and update the mapping; do not report an unconditional PASS.
