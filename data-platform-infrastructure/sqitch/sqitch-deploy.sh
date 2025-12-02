# !/bin/bash

# Usage:
#   ./sqitch-deploy.sh <host> <user> <password> [<database>]
#
# Example:
#   ./sqitch-deploy.sh localhost myuser mypass mydb

HOST=$1
USER=$2
PASS=$3
DBNAME=${4:-"3phi-db"}   # default database name is "postgres"

if [ -z "$HOST" ] || [ -z "$USER" ] || [ -z "$PASS" ]; then
  echo "Usage: $0 <host> <user> <password> [<database>]"
  exit 1
fi

# Construct database URI
DB_URI="db:pg://$USER:$PASS@$HOST/$DBNAME"

echo "Running sqitch deploy to $DB_URI ..."
sqitch deploy "$DB_URI"
