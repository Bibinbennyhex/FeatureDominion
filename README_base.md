# Summary Version 4

`summary_version_4` is an isolated pipeline package based on baseline semantics with targeted Case III optimizations:

- latest/hot/cold Case III split
- hot lane uses latest table context (`latest_summary`)
- cold lane uses legacy path with forced broadcast guard
- v4 table contract validation and bootstrap tooling

## Files

- `summary_inc_v4.py`: v4 pipeline entrypoint
- `config_v4.json`: v4 config template
- `scripts/bootstrap_latest_summary_v4_from_summary.py`: bootstrap `latest_summary` 72-month arrays from `summary`
- `scripts/validate_latest_summary_v4.py`: v4 contract validator
- `tests/`: lightweight v4 validation tests

## Key Config Flags

- `latest_history_table`
- `latest_history_window_months` (default `72`)
- `enable_case3_hot_cold_split` (default `true`)
- `case3_hot_window_months` (default `36`)
- `force_cold_case3_broadcast` (default `true`)
- `cold_case3_broadcast_row_cap` (default `10000000`)

## Run Bootstrap

```powershell
python main/summary_version_4/scripts/bootstrap_latest_summary_v4_from_summary.py --config main/summary_version_4/config_v4.json --replace
```

## Validate V4 Table

```powershell
python main/summary_version_4/scripts/validate_latest_summary_v4.py --config main/summary_version_4/config_v4.json
```

## Run Pipeline

Use the same invocation style as existing pipeline with the v4 script and v4 config.


$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$logDir = "main/summary_version_4/tests/artifacts/manual_full/run_$ts"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

# Restart all containers in the compose file
docker compose -f main/docker_test/docker-compose.yml down --remove-orphans
docker compose -f main/docker_test/docker-compose.yml up -d

# Run full V4 suite: logs in shell + saved file
$logFile = Join-Path $logDir "run_v4_tests_sequential.log"
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main `
  python3 /workspace/main/summary_version_4/tests/run_v4_tests_sequential.py 2>&1 `
  | Tee-Object -FilePath $logFile

Write-Host "Saved log: $logFile"


docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main `
  python3 /workspace/main/summary_version_4/tests/test_v4_case_end_to_end_smoke.py 2>&1 `



docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main `
  python3 /workspace/main/summary_version_4/tests/test_v4_summary36_latest72_window_update.py 2>&1 `

# Use v4.1
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main `
  python3 /workspace/main/summary_version_4/tests/run_v4_tests_sequential.py `
  --pipeline-script summary_inc_v4.1.py

# Use v4
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main `
  python3 /workspace/main/summary_version_4/tests/run_v4_tests_sequential.py `
  --pipeline-script summary_inc_v4.py


docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests/test_v4_case_end_to_end_smoke.py ` --pipeline-script summary_inc_v5.py

docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests/run_v4_tests_sequential.py ` --pipeline-script summary_inc_v5.py


docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests/summary_inc_v5_tests/summary_inc_v5_tests/run_v5_tests_sequential.py ` --pipeline-script summary_inc_v5.py


docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests_clone/test_v4_summary36_latest72_window_update.py ` --pipeline-script summary_inc_v4.1.py

docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests_clone/test_v4_case3_full_contract_validation.py ` --pipeline-script summary_inc_v4.1.py


docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests_clone/test_v4_case3_comprehensive.py ` --pipeline-script summary_inc_v4.1.py


docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests_clone/run_v4_tests_sequential.py ` --pipeline-script summary_inc_v4.1.py

docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main ` python3 /workspace/main/summary_version_4/tests_clone/test_v4_case3_comprehensive.py ` --pipeline-script summary_inc_v4.py


docker compose -f main/docker_test/docker-compose.yml exec -T -e V4_PIPELINE_SCRIPT=summary_inc_v4.py spark-iceberg-main python3 /workspace/main/summary_version_4/tests/test_v4_case3_comprehensive.py


docker compose -f main/docker_test/docker-compose.yml exec -T -e V4_PIPELINE_SCRIPT=summary_inc_v4.py spark-iceberg-main python3 /workspace/main/summary_version_4/tests_clone/test_v4_case3_comprehensive.py