#!/usr/bin/env bash
set -e

until pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-airflow}"; do
    echo "Waiting for Postgres..."
    sleep 2
done

airflow db migrate

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

python /opt/airflow/init/init_connectors.py

exec "$@"