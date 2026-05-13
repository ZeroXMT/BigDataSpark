"""
ETL job #2: star schema (PostgreSQL) -> 6 витрин (ClickHouse)

Запуск:
    spark-submit \
      --jars /opt/spark-jars/postgresql-42.7.4.jar,/opt/spark-jars/clickhouse-jdbc-0.6.5-shaded.jar \
      02_etl_to_clickhouse.py

Нужны JARы в spark/jars/:
  - postgresql-42.7.4.jar
  - clickhouse-jdbc-0.6.5-shaded.jar
"""

from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("ETL: star schema -> ClickHouse reports")
    .config("spark.jars",
            "/opt/spark-jars/postgresql-42.7.4.jar,"
            "/opt/spark-jars/clickhouse-jdbc-0.6.5-shaded.jar")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# --- PostgreSQL (источник: star schema) ---
PG_URL = "jdbc:postgresql://postgres:5432/mydatabase"
PG_PROPS = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# --- ClickHouse (приёмник: витрины) ---
CH_URL = "jdbc:clickhouse://clickhouse:8123/reports"
CH_PROPS = {
    "user": "clickhouse",
    "password": "password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "socket_timeout": "300000",
    "isolationLevel": "NONE"
}


def read_pg(table):
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)


def write_ch(df, table, order_by="tuple()"):
    df_cached = df.cache()
    count = df_cached.count()
    (df_cached.write
       .mode("overwrite")
       .option("createTableOptions", f"ENGINE = MergeTree() ORDER BY {order_by}")
       .jdbc(CH_URL, table, properties=CH_PROPS))
    print(f"[{table}] записано: {count}")
    df_cached.unpersist()


# читаем все таблицы star-схемы
fact   = read_pg("star.fact_sales")
d_prod = read_pg("star.dim_product")
d_cat  = read_pg("star.dim_product_category")
d_cust = read_pg("star.dim_customer")
d_sell = read_pg("star.dim_seller")
d_sup  = read_pg("star.dim_supplier")
d_stor = read_pg("star.dim_store")
d_date = read_pg("star.dim_date")

# регистрируем как temp views для Spark SQL
fact.createOrReplaceTempView("fact_sales")
d_prod.createOrReplaceTempView("dim_product")
d_cat.createOrReplaceTempView("dim_product_category")
d_cust.createOrReplaceTempView("dim_customer")
d_sell.createOrReplaceTempView("dim_seller")
d_sup.createOrReplaceTempView("dim_supplier")
d_stor.createOrReplaceTempView("dim_store")
d_date.createOrReplaceTempView("dim_date")


# ======== Витрина 1: Продажи по товарам ========
mart1 = spark.sql("""
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
""")
write_ch(mart1, "mart_sales_by_product", order_by="revenue_rank")


# ======== Витрина 2: Продажи по клиентам ========
mart2 = spark.sql("""
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
""")
write_ch(mart2, "mart_sales_by_customer", order_by="revenue_rank")


# ======== Витрина 2 (доп.): Распределение клиентов по странам ========
mart2b = spark.sql("""
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
""")
write_ch(mart2b, "mart_customers_by_country", order_by="country_rank")


# ======== Витрина 3: Продажи по времени ========
# Строки с month=0 — годовые итоги; остальные — помесячные.
mart3 = spark.sql("""
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
""")
write_ch(mart3, "mart_sales_by_time", order_by="(year, month)")


# ======== Витрина 4: Продажи по магазинам ========
mart4 = spark.sql("""
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
""")
write_ch(mart4, "mart_sales_by_store", order_by="revenue_rank")


# ======== Витрина 5: Продажи по поставщикам ========
mart5 = spark.sql("""
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
""")
write_ch(mart5, "mart_sales_by_supplier", order_by="revenue_rank")


# ======== Витрина 5 (доп.): Распределение продаж по странам поставщиков ========
mart5b = spark.sql("""
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
""")
write_ch(mart5b, "mart_suppliers_by_country", order_by="country_rank")


# ======== Витрина 6: Качество товаров ========
mart6 = spark.sql("""
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
""")
write_ch(mart6, "mart_product_quality", order_by="rating_rank")


print("\nВсе витрины загружены в ClickHouse!")
spark.stop()
