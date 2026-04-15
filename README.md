# Restart all containers in the compose file
docker compose -f ./docker/docker-compose.yml down --remove-orphans
docker compose -f ./docker/docker-compose.yml up -d

