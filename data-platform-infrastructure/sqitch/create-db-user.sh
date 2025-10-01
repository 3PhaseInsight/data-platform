#!/usr/bin/env bash

# Simple local script to create a Postgres role in the running 'timescaledb' container.
# Usage:
#   ./create-db-user.sh <username> <password>

# Exit if any command returns a non-zero status
set -eo pipefail

# Check number of arguments, should be 2
if [ "$#" -ne 2 ]; then
  echo "Provide two arguments. Example of usage: $0 <username> <password>"
  exit 1
fi

USER="$1"
PASS="$2"

# Check if a container exists with the name timescaledb
if ! docker ps --format '{{.Names}}' | grep -q '^timescaledb$'; then
  echo "Error: 'timescaledb' container is not running. Start it with: make up"
  exit 1
fi

# Check if role exists
EXISTS=$(docker exec timescaledb bash -lc \
  "export PGPASSWORD=\"\$POSTGRES_PASSWORD\"; \
   psql -q -h 127.0.0.1 -p 5432 -U \"\$POSTGRES_USER\" -d \"\${POSTGRES_DB:-postgres}\" -tAc \"SELECT 1 FROM pg_roles WHERE rolname = '$USER';\"" \
  | tr -d '[:space:]')

if [ "$EXISTS" = "1" ]; then
  echo "Role '$USER' already exists." 
  exit 0
fi

# Create role
docker exec timescaledb bash -lc \
  "export PGPASSWORD=\"\$POSTGRES_PASSWORD\"; \
   psql -h 127.0.0.1 -p 5432 -U \"\$POSTGRES_USER\" -d \"\${POSTGRES_DB:-postgres}\" -c \"CREATE ROLE $USER WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION PASSWORD '$PASS';\""

echo "Role '$USER' created."