# Warehouse Validation — Step 11.7

- Source: `lakehouse.gold.hourly_hydrology`
- Warehouse: `lakehouse.dw` (8 dimensions, 4 facts)
- Fact grain: `hour_start × location_key`
- Logical foreign keys are validated by application checks, not enforced by Iceberg.
- `dim_region` intentionally empty until verified metadata is available.
- No filling of missing hydrological measurements during warehouse loading.
- Step 11.6 status: **PASS**
- Step 11.7 status: **PASS**

## Snapshot idempotency comparison

| Fact | Before | After | Result |
|---|---:|---:|---|
| `fact_hydrology` | 1 | 1 | PASS |
| `fact_water_level` | 1 | 1 | PASS |
| `fact_rainfall` | 1 | 1 | PASS |
| `fact_data_quality` | 1 | 1 | PASS |

## Interpretation

Matching current snapshot IDs and full snapshot histories before and after a verified `VERIFY_NOOP` rerun support physical no-write idempotency for the observed run.
This does not establish that the entire pipeline is transactional across tables.

## Evidence files

- `iceberg_snapshots_before.json`
- `iceberg_snapshots_after.json`
- `warehouse_validation.json`
- `data_dictionary.md`
- Step 11.6 `warehouse_dq_*.json`
- Step 11.5 rerun `facts_*.json`
