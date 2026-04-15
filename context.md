# Context: Summary Incremental V4

## Source Inputs
- `conversation/conversation.jsonl`
- `src/summary_inc_v4.py`
- `config/pipeline.json`

## Conversation Snapshot (from `conversation.jsonl`)
- Time window: `2026-04-10T14:27:51Z` to `2026-04-10T16:22:54Z`.
- Extracted message counts: `592` user messages, `1605` assistant messages.
- Dominant focus areas (keyword frequency in user messages):
- `test` (`3136`), `case3` (`222`), `hot` (`229`), `cold` (`203`), `summary_inc_v4` (`166`), `idempotency` (`103`), `case iii` (`87`).
- Repeated user intent themes:
- Hardening Case III behavior (hot/cold split, mixed-lane handling, overlap safety).
- Soft-delete correctness for Case III.
- Performance and workflow tuning.
- Strong emphasis on automated validation and targeted test runs.
- Discussion later shifts toward a lean `summary_version_6` design while preserving proven V4 behavior.

## Pipeline Config Contract (`config/pipeline.json`)
- Core tables:
- Source: `spark_catalog.edf_gold.ivaps_consumer_accounts_all`
- Summary target: `primary_catalog.edf_gold.summary`
- Latest target: `primary_catalog.edf_gold.latest_summary`
- Hist table used in prep: `primary_catalog.edf_gold.consumer_account_hist_rpt`
- Keys and drivers:
- PK: `cons_acct_key`
- Partition month: `rpt_as_of_mo`
- Increment watermark column: `base_ts`
- History lengths:
- Summary history length: `36`
- Latest history window months: `72`
- Case III/III-delete split controls:
- `enable_case3_hot_cold_split: true`
- `case3_hot_window_months: 36`
- `force_case3_unified_on_any_overlap: false`
- `force_case3d_unified_on_any_overlap: false`
- Context optimization controls:
- `use_working_set_latest_context: true`
- `use_working_set_case3_summary_context: true`
- `use_latest_history_context_case3: true`
- `use_latest_history_context: true`
- Cold-lane broadcast guard:
- `force_cold_case3_broadcast: true`
- `cold_case3_broadcast_row_cap: 10000000`
- Validation toggles:
- `validate_latest_history_window: true`
- `validate_latest_history_uniqueness: true`

## `summary_inc_v4.py` Architecture Summary

### High-level flow (`run_pipeline`)
- Run-start safety:
- Ensures soft delete column availability.
- Preloads target column metadata.
- Captures pre-run snapshot state.
- Marks tracker status as `RUNNING` with run id.
- Step 1: load + classify increment records into `CASE_I`, `CASE_II`, `CASE_III`, `CASE_IV` and tag `_is_soft_delete`.
- Step 2 processing order (explicit and correctness-critical):
- `CASE_III` backfill first.
- `CASE_I` new accounts next.
- `CASE_IV` bulk-historical next.
- `CASE_II` forward updates last.
- Finalization:
- On success: finalize tracker as `SUCCESS` and log post-run snapshot.
- On failure: rollback summary/latest tables to pre-run snapshots, finalize tracker as `FAILURE` with rollback status.

### Increment watermark model
- Source filtering does not rely only on destination max timestamp.
- Pipeline reads `committed_ingestion_watermark` from tracker when present.
- Falls back to summary max timestamp if committed watermark is absent.
- This supports transactional advancement only after end-to-end success.

### Classification model (`load_and_classify_accounts`)
- Validates summary and latest table max month consistency.
- Reads both main source table and `hist_rpt_dt` table with `base_ts > effective watermark`.
- Applies mapping/transforms/inferred fields and dedupes.
- Classification rules:
- `CASE_I`: account not in latest summary.
- `CASE_II`: account exists and incoming month is newer.
- `CASE_III`: account exists and incoming month is same/older.
- `CASE_IV`: for new accounts with multiple months in one batch, earliest month is `CASE_I`, later months become `CASE_IV`.

### Case III (backfill) strategy
- Supports split mode by month recency when enabled:
- Hot lane: recent months (within configured window), prefers latest-history-context path.
- Cold lane: older months, falls back to legacy summary-scan path.
- Mixed lane: accounts spanning both hot and cold.
- Overlap handling:
- If overlap exists and `force_case3_unified_on_any_overlap=true`, pipeline uses unified non-split path.
- Else it processes mixed/hot-only/cold-only lanes separately, snapshots temporary outputs, then combines.
- Broadcast guard for cold processing exists via row cap.

### Case III soft-delete strategy
- Soft delete behavior for existing months:
- Mark target month row with `soft_del_cd`.
- Nullify deleted month position in future history arrays.
- Uses the same hot/cold/mixed lane split pattern and overlap safety option for Case3-delete (`force_case3d_unified_on_any_overlap`).

### Working-set context optimization
- Builds scoped context tables from classified keys:
- `execution_catalog.checkpointdb.workset_latest_summary`
- `execution_catalog.checkpointdb.workset_summary_case3`
- Reduces full-table scans during joins, especially for Case III paths.

### Tracker + rollback model
- Tracker table records:
- `summary`
- `latest_summary`
- `committed_ingestion_watermark`
- Status lifecycle: `RUNNING` -> `SUCCESS` or `FAILURE`.
- On exceptions after run start, best-effort rollback to captured Iceberg snapshot ids is executed for both summary and latest tables.

### Temporary/patch tables used
- Case temp outputs and patch layers are written in `execution_catalog.checkpointdb`, including:
- `case_1`, `case_2`, `case_3a`, `case_3b`, `case_4`
- Case III patch tables (`case_3_latest_month_patch`, unified variants)
- Case III delete patch tables (`case_3d_month`, `case_3d_future`, context/unified patch tables)
- Working-set helper tables (`workset_latest_summary`, `workset_summary_case3`)

## Practical Takeaways
- V4 is optimized for correctness under mixed incremental patterns, not minimal complexity.
- Conversation history shows most risk and testing effort concentrated in Case III (including soft deletes and mixed hot/cold scenarios).
- If evolving this pipeline (for example into V6), preserve:
- Watermark commit semantics.
- Run-start snapshot capture + rollback.
- Case III lane safety behavior.
- Test coverage around mixed hot/cold + idempotency.
