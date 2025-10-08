#!/usr/bin/env bash

# Simple local script to create a Postgres role without depending on a specific container runtime.
# Usage:
#      ./create-db-user.sh <host/IP> <port> <admin_user_of_db> <admin_password> <role> <role_password> [<database_name>]
#      (database_name is optional, defaults to POSTGRES_DB environment variable or if not set it defaults to "postgres")
#      e.g. ./create-db-user.sh localhost 5432 postgres password threephi_db_user strongpass 3phi-db

set -eo pipefail

# Check argument count (should be 6 or 7)
if [ "$#" -lt 6 ] || [ "$#" -gt 7 ]; then
  echo "Usage: $0 <host/IP> <port> <admin_user_of_db> <admin_password> <role> <role_password> [<database_name>]"
  exit 1
fi

# Check if psql is installed
if ! command -v psql >/dev/null 2>&1; then
  echo "Error: psql command not found. Install the PostgreSQL client tools before running this script. If already installed, make sure it's in your PATH."
  exit 1
fi

# Assign arguments to variables
HOST="$1"
PORT="$2"
ADMIN_USER="$3"
ADMIN_PASS="$4"
ROLE="$5"
ROLE_PASS="$6"
DB_NAME="${7:-${POSTGRES_DB:-postgres}}"

# Trim whitespace from psql output before comparing.
trim() {
  tr -d '[:space:]'
}

# Check if role already exists.
EXISTS=$(PGPASSWORD="$ADMIN_PASS" \
  psql -q -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d "$DB_NAME" \
  -tAc "SELECT 1 FROM pg_roles WHERE rolname = '$ROLE';" | trim || true)

if [ "$EXISTS" = "1" ]; then
  echo "Role '$ROLE' already exists."
  exit 0
fi

# Escape single quotes in the password for SQL string literal safety.
ESCAPED_ROLE_PASS=${ROLE_PASS//\'/''}

# Create the role with the specified password.
PGPASSWORD="$ADMIN_PASS" \
  psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d "$DB_NAME" -c \
    "CREATE ROLE \"$ROLE\" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION PASSWORD '$ESCAPED_ROLE_PASS';"

echo "Role '$ROLE' created."
