"""
ETL job #3: star schema (PostgreSQL) -> 6 витрин (Cassandra)

Запуск:
    spark-submit \
      --jars /opt/spark-jars/postgresql-42.7.4.jar,/opt/spark-jars/spark-cassandra-connector-assembly_2.12-3.5.0.jar \
      jobs/03_etl_to_cassandra.py

DBeaver: New Connection → Apache Cassandra
  Host: localhost  Port: 9042  (без пользователя/пароля по умолчанию)
  Keyspace: reports
"""

import importlib.util
import subprocess
import sys

# cassandra-driver нужен только на драйвере для DDL (keyspace/таблицы перед записью Spark).
# Устанавливаем только если пакет ещё не доступен — идемпотентно при повторных запусках.
if importlib.util.find_spec("cassandra") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "cassandra-driver"])

from cassandra.cluster import Cluster

cluster = Cluster(["cassandra"])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS reports
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace("reports")

_ddls = [
    """CREATE TABLE IF NOT EXISTS mart_sales_by_product (
        product_id     INT,
        product_name   TEXT,
        category_name  TEXT,
        total_revenue  DOUBLE,
        total_quantity BIGINT,
        avg_rating     DOUBLE,
        total_reviews  INT,
        revenue_rank   INT,
        PRIMARY KEY (product_id)
    )""",
    """CREATE TABLE IF NOT EXISTS mart_sales_by_customer (
        customer_id   INT,
        customer_name TEXT,
        country       TEXT,
        total_orders  BIGINT,
        total_revenue DOUBLE,
        avg_basket    DOUBLE,
        revenue_rank  INT,
        PRIMARY KEY (customer_id)
    )""",
    """CREATE TABLE IF NOT EXISTS mart_customers_by_country (
        country        TEXT PRIMARY KEY,
        customer_count BIGINT,
        total_orders   BIGINT,
        total_revenue  DOUBLE,
        avg_basket     DOUBLE,
        country_rank   INT
    )""",
    # month=0 — годовой итог; partition key = year, clustering key = month
    """CREATE TABLE IF NOT EXISTS mart_sales_by_time (
        year            SMALLINT,
        month           SMALLINT,
        period_type     TEXT,
        total_orders    BIGINT,
        total_revenue   DOUBLE,
        avg_order_value DOUBLE,
        PRIMARY KEY (year, month)
    )""",
    """CREATE TABLE IF NOT EXISTS mart_sales_by_store (
        store_id      INT,
        store_name    TEXT,
        city          TEXT,
        country       TEXT,
        total_orders  BIGINT,
        total_revenue DOUBLE,
        avg_basket    DOUBLE,
        revenue_rank  INT,
        PRIMARY KEY (store_id)
    )""",
    """CREATE TABLE IF NOT EXISTS mart_sales_by_supplier (
        supplier_id    INT,
        supplier_name  TEXT,
        country        TEXT,
        total_revenue  DOUBLE,
        avg_item_price DOUBLE,
        product_count  BIGINT,
        revenue_rank   INT,
        PRIMARY KEY (supplier_id)
    )""",
    """CREATE TABLE IF NOT EXISTS mart_suppliers_by_country (
        supplier_country TEXT PRIMARY KEY,
        supplier_count   BIGINT,
        product_count    BIGINT,
        total_revenue    DOUBLE,
        country_rank     INT
    )""",
    """CREATE TABLE IF NOT EXISTS mart_product_quality (
        product_id    INT,
        product_name  TEXT,
        category_name TEXT,
        rating        DOUBLE,
        reviews       INT,
        sales_count   BIGINT,
        total_revenue DOUBLE,
        rating_rank   INT,
        PRIMARY KEY (product_id)
    )""",
]

for ddl in _ddls:
    session.execute(ddl)

cluster.shutdown()
print("Keyspace and tables ready in Cassandra.")

# ---------- Spark ----------
from pyspark.sql import SparkSession

CASS_JAR = "/opt/spark-jars/spark-cassandra-connector-assembly_2.12-3.5.0.jar"
PG_JAR   = "/opt/spark-jars/postgresql-42.7.4.jar"

spark = (SparkSession.builder
    .appName("ETL: star schema -> Cassandra")
    .config("spark.jars", f"{PG_JAR},{CASS_JAR}")
    .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/mydatabase"
PG_PROPS = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


def read_pg(table):
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)


def write_cass(df, table):
    df_cached = df.cache()
    count = df_cached.count()
    (df_cached.write
       .format("org.apache.spark.sql.cassandra")
       .mode("overwrite")
       .option("keyspace", "reports")
       .option("table", table)
       .option("confirm.truncate", "true")
       .save())
    print(f"[{table}] записано: {count}")
    df_cached.unpersist()


fact   = read_pg("star.fact_sales")
d_prod = read_pg("star.dim_product")
d_cat  = read_pg("star.dim_product_category")
d_cust = read_pg("star.dim_customer")
d_stor = read_pg("star.dim_store")
d_sup  = read_pg("star.dim_supplier")
d_date = read_pg("star.dim_date")

fact.createOrReplaceTempView("fact_sales")
d_prod.createOrReplaceTempView("dim_product")
d_cat.createOrReplaceTempView("dim_product_category")
d_cust.createOrReplaceTempView("dim_customer")
d_stor.createOrReplaceTempView("dim_store")
d_sup.createOrReplaceTempView("dim_supplier")
d_date.createOrReplaceTempView("dim_date")


# ======== Витрина 1 ========
write_cass(spark.sql("""
    SELECT
        p.product_id,
        p.name                              AS product_name,
        COALESCE(pc.category_name, 'N/A')  AS category_name,
        ROUND(SUM(f.total_price), 2)        AS total_revenue,
        SUM(f.quantity)                     AS total_quantity,
        ROUND(AVG(COALESCE(p.rating, 0)), 2)   AS avg_rating,
        MAX(COALESCE(p.reviews, 0))         AS total_reviews,
        RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    LEFT JOIN dim_product_category pc ON p.category_id = pc.category_id
    GROUP BY p.product_id, p.name, pc.category_name
"""), "mart_sales_by_product")


# ======== Витрина 2 ========
write_cass(spark.sql("""
    SELECT
        c.customer_id,
        CONCAT(COALESCE(c.first_name,''), ' ', COALESCE(c.last_name,'')) AS customer_name,
        COALESCE(c.country, 'Unknown')     AS country,
        COUNT(f.sale_id)                   AS total_orders,
        ROUND(SUM(f.total_price), 2)       AS total_revenue,
        ROUND(AVG(f.total_price), 2)       AS avg_basket,
        RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.country
"""), "mart_sales_by_customer")


# ======== Витрина 2 (доп.): Распределение по странам ========
write_cass(spark.sql("""
    SELECT
        COALESCE(c.country, 'Unknown')               AS country,
        COUNT(DISTINCT c.customer_id)                AS customer_count,
        COUNT(f.sale_id)                             AS total_orders,
        ROUND(SUM(f.total_price), 2)                 AS total_revenue,
        ROUND(AVG(f.total_price), 2)                 AS avg_basket,
        RANK() OVER (ORDER BY COUNT(DISTINCT c.customer_id) DESC) AS country_rank
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY c.country
"""), "mart_customers_by_country")


# ======== Витрина 3: Продажи по времени ========
write_cass(spark.sql("""
    SELECT
        d.year,
        d.month,
        'monthly'                    AS period_type,
        COUNT(f.sale_id)             AS total_orders,
        ROUND(SUM(f.total_price), 2) AS total_revenue,
        ROUND(AVG(f.total_price), 2) AS avg_order_value
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year, d.month

    UNION ALL

    SELECT
        d.year,
        CAST(0 AS SMALLINT)          AS month,
        'yearly'                     AS period_type,
        COUNT(f.sale_id)             AS total_orders,
        ROUND(SUM(f.total_price), 2) AS total_revenue,
        ROUND(AVG(f.total_price), 2) AS avg_order_value
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year
    ORDER BY year, month
"""), "mart_sales_by_time")


# ======== Витрина 4: Продажи по магазинам ========
write_cass(spark.sql("""
    SELECT
        st.store_id,
        st.name                            AS store_name,
        COALESCE(st.city, 'Unknown')       AS city,
        COALESCE(st.country, 'Unknown')    AS country,
        COUNT(f.sale_id)                   AS total_orders,
        ROUND(SUM(f.total_price), 2)       AS total_revenue,
        ROUND(AVG(f.total_price), 2)       AS avg_basket,
        RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_store st ON f.store_id = st.store_id
    GROUP BY st.store_id, st.name, st.city, st.country
"""), "mart_sales_by_store")


# ======== Витрина 5: Продажи по поставщикам ========
write_cass(spark.sql("""
    SELECT
        s.supplier_id,
        s.name                             AS supplier_name,
        COALESCE(s.country, 'Unknown')     AS country,
        ROUND(SUM(f.total_price), 2)       AS total_revenue,
        ROUND(AVG(p.price), 2)             AS avg_item_price,
        COUNT(DISTINCT p.product_id)       AS product_count,
        RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_product p  ON f.product_id  = p.product_id
    JOIN dim_supplier s ON p.supplier_id = s.supplier_id
    GROUP BY s.supplier_id, s.name, s.country
"""), "mart_sales_by_supplier")


# ======== Витрина 5 (доп.): Распределение продаж по странам поставщиков ========
write_cass(spark.sql("""
    SELECT
        COALESCE(s.country, 'Unknown')     AS supplier_country,
        COUNT(DISTINCT s.supplier_id)      AS supplier_count,
        COUNT(DISTINCT p.product_id)       AS product_count,
        ROUND(SUM(f.total_price), 2)       AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.total_price) DESC) AS country_rank
    FROM fact_sales f
    JOIN dim_product p  ON f.product_id  = p.product_id
    JOIN dim_supplier s ON p.supplier_id = s.supplier_id
    GROUP BY s.country
"""), "mart_suppliers_by_country")


# ======== Витрина 6: Качество товаров ========
write_cass(spark.sql("""
    SELECT
        p.product_id,
        p.name                              AS product_name,
        COALESCE(pc.category_name, 'N/A')  AS category_name,
        COALESCE(p.rating, 0)              AS rating,
        COALESCE(p.reviews, 0)             AS reviews,
        COUNT(f.sale_id)                   AS sales_count,
        ROUND(SUM(f.total_price), 2)       AS total_revenue,
        RANK() OVER (ORDER BY COALESCE(p.rating, 0) DESC) AS rating_rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    LEFT JOIN dim_product_category pc ON p.category_id = pc.category_id
    GROUP BY p.product_id, p.name, pc.category_name, p.rating, p.reviews
"""), "mart_product_quality")


print("\nВсе витрины загружены в Cassandra!")
spark.stop()
