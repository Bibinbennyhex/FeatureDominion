# Restart all containers in the compose file
docker compose -f ./docker/docker-compose.yml down --remove-orphans
docker compose -f ./docker/docker-compose.yml up -d

docker compose -f docker/docker-compose.yml exec -T spark-iceberg-main python3 '/workspace/Feature _Deployment/regression_suite/tests/run_all_tests_v4.py' --tier smoke
docker compose -f docker/docker-compose.yml exec -T spark-iceberg-main python3 '/workspace/Feature _Deployment/regression_suite/tests/run_all_tests_v4.py' --tier nightly


docker compose -f docker/docker-compose.yml exec -T spark-iceberg-main python3 '/workspace/Feature _Deployment/regression_suite/tests/test_v4_case3_comprehensive.py








