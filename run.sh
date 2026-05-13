#!/usr/bin/env bash
# Полный запуск ETL-пайплайна: поднимает стек и прогоняет все три джобы.
set -euo pipefail

PG_JAR="/opt/spark-jars/postgresql-42.7.4.jar"
CH_JAR="/opt/spark-jars/clickhouse-jdbc-0.6.5-shaded.jar"
CASS_JAR="/opt/spark-jars/spark-cassandra-connector-assembly_2.12-3.5.0.jar"

echo "=== [1/4] Поднимаем контейнеры ==="
docker compose up -d

echo "=== [2/4] Ждём готовности сервисов ==="
until docker compose exec -T postgres pg_isready -U postgres -d mydatabase -q; do sleep 3; done
echo "  PostgreSQL ready"
until docker compose exec -T clickhouse wget --spider -q http://localhost:8123/ping 2>/dev/null; do sleep 3; done
echo "  ClickHouse ready"
until docker compose exec -T cassandra cqlsh -e 'describe keyspaces' >/dev/null 2>&1; do sleep 10; done
echo "  Cassandra ready"

echo "=== [3/4] ETL #1: mock_data -> star schema (PostgreSQL) ==="
docker compose exec -T spark spark-submit \
  --jars "$PG_JAR" \
  work/jobs/01_etl_to_star.py

echo "=== [4a/4] ETL #2: star schema -> витрины ClickHouse ==="
docker compose exec -T spark spark-submit \
  --jars "$PG_JAR,$CH_JAR" \
  work/jobs/02_etl_to_clickhouse.py

echo "=== [4b/4] ETL #3: star schema -> витрины Cassandra ==="
docker compose exec -T spark spark-submit \
  --jars "$PG_JAR,$CASS_JAR" \
  work/jobs/03_etl_to_cassandra.py

echo ""
echo "=== Проверка: количество строк в PostgreSQL ==="
docker compose exec -T postgres psql -U postgres -d mydatabase \
  -c "SELECT count(*) AS staging_rows FROM mock_data;" \
  -c "SELECT count(*) AS fact_rows    FROM star.fact_sales;"

echo ""
echo "=== Проверка: таблицы в ClickHouse ==="
docker compose exec -T clickhouse clickhouse-client \
  --user clickhouse --password password \
  --query "SELECT table, count() AS rows FROM system.tables WHERE database='reports' GROUP BY table ORDER BY table;"

echo ""
echo "=== Проверка: таблицы в Cassandra ==="
docker compose exec -T cassandra cqlsh -e \
  "SELECT table_name FROM system_schema.tables WHERE keyspace_name='reports';"

echo ""
echo "Готово! Все ETL-джобы выполнены успешно."
