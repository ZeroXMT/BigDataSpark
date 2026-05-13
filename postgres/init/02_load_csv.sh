#!/usr/bin/env bash
# Loads all CSV files mounted at /csv into the staging table mock_data.
# Runs once on first container start (docker-entrypoint-initdb.d).
set -euo pipefail

echo "Loading CSV files from /csv into mock_data ..."

for f in /csv/MOCK_DATA*.csv; do
    [ -e "$f" ] || continue
    echo "  -> $f"
    psql -v ON_ERROR_STOP=1 \
         --username "$POSTGRES_USER" \
         --dbname  "$POSTGRES_DB" \
         -c "\copy mock_data FROM '$f' WITH (FORMAT csv, HEADER true, NULL '')"
done

count=$(psql -At --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "SELECT count(*) FROM mock_data;")
echo "mock_data row count: $count"
