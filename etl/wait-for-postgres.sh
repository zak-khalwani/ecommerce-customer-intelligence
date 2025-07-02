#!/bin/sh
# wait-for-postgres.sh

set -e

# The 'DB_HOST' variable will be passed from docker-compose
host="$DB_HOST"

until pg_isready -h "$host" -U "$DB_USER"; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"
exec "$@"
