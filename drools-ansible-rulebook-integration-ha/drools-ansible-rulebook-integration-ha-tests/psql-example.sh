#!/bin/bash

echo "Connecting to PostgreSQL database in Docker container..."

docker exec -it eda-ha-postgres psql -U eda_user -d eda_ha_db