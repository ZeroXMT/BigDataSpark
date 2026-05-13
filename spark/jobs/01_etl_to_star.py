"""
ETL job #1: mock_data (PostgreSQL) - star schema (PostgreSQL)

Запуск из Jupyter или через spark-submit:
    spark-submit --jars /opt/spark-jars/postgresql-42.7.4.jar 01_etl_to_star.py

Нужен JAR: spark/jars/postgresql-42.7.4.jar
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("ETL: mock_data -> star schema")
    .config("spark.jars", "/opt/spark-jars/postgresql-42.7.4.jar")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://postgres:5432/mydatabase"
PG_PROPS = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# читаем staging-таблицу целиком
raw = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)
raw.cache()
print(f"[staging] строк: {raw.count()}")


# ---------- dim_pet_type ----------
dim_pet_type = (
    raw.select("customer_pet_type", "pet_category")
       .filter(F.col("customer_pet_type").isNotNull() & (F.col("customer_pet_type") != ""))
       .dropDuplicates(["customer_pet_type", "pet_category"])
       .withColumn("pet_type_id",
                   F.row_number().over(Window.orderBy("customer_pet_type", "pet_category")))
       .select(
           F.col("pet_type_id"),
           F.col("customer_pet_type").alias("pet_type"),
           F.col("pet_category")
       )
)
dim_pet_type.write.jdbc(PG_URL, "star.dim_pet_type", mode="overwrite", properties=PG_PROPS)
print(f"[dim_pet_type] записано: {dim_pet_type.count()}")


# ---------- dim_customer ----------
# нужен pet_type_id - джойним с только что сохранённым dim_pet_type
# готовим dim_pet_type для джойна: переименовываем колонки чтобы не было коллизий
pet_type_for_join = (
    dim_pet_type
    .withColumnRenamed("pet_type", "_pt_type")
    .withColumn("_pt_cat", F.coalesce(F.col("pet_category"), F.lit("")))
    .drop("pet_category")
)

dim_customer = (
    raw.select(
        F.col("sale_customer_id").cast("int").alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.when(F.col("customer_age") != "", F.col("customer_age").cast("int")).alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type"),
        F.coalesce(F.col("pet_category"), F.lit("")).alias("_cust_pet_cat"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed")
    )
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .join(
        pet_type_for_join,
        (F.col("customer_pet_type") == F.col("_pt_type")) &
        (F.col("_cust_pet_cat") == F.col("_pt_cat")),
        "left"
    )
    .select("customer_id", "first_name", "last_name", "age", "email",
            "country", "postal_code", "pet_type_id", "pet_name", "pet_breed")
)
dim_customer.write.jdbc(PG_URL, "star.dim_customer", mode="overwrite", properties=PG_PROPS)
print(f"[dim_customer] записано: {dim_customer.count()}")


# ---------- dim_seller ----------
dim_seller = (
    raw.select(
        F.col("sale_seller_id").cast("int").alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code")
    )
    .filter(F.col("seller_id").isNotNull())
    .dropDuplicates(["seller_id"])
)
dim_seller.write.jdbc(PG_URL, "star.dim_seller", mode="overwrite", properties=PG_PROPS)
print(f"[dim_seller] записано: {dim_seller.count()}")


# ---------- dim_product_category ----------
dim_cat = (
    raw.select(F.col("product_category").alias("category_name"))
       .filter(F.col("category_name").isNotNull() & (F.col("category_name") != ""))
       .dropDuplicates()
       .withColumn("category_id",
                   F.row_number().over(Window.orderBy("category_name")))
       .select("category_id", "category_name")
)
dim_cat.write.jdbc(PG_URL, "star.dim_product_category", mode="overwrite", properties=PG_PROPS)
print(f"[dim_product_category] записано: {dim_cat.count()}")


# ---------- dim_supplier ----------
dim_supplier = (
    raw.select(
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.coalesce(F.col("supplier_city"), F.lit("")).alias("city"),
        F.col("supplier_country").alias("country")
    )
    .filter(F.col("name").isNotNull() & (F.col("name") != ""))
    .dropDuplicates(["name", "city"])
    .withColumn("supplier_id",
                F.row_number().over(Window.orderBy("name", "city")))
    .select("supplier_id", "name", "contact", "email", "phone", "address", "city", "country")
)
dim_supplier.write.jdbc(PG_URL, "star.dim_supplier", mode="overwrite", properties=PG_PROPS)
print(f"[dim_supplier] записано: {dim_supplier.count()}")


# ---------- dim_product ----------
dim_product_raw = (
    raw.select(
        F.col("sale_product_id").cast("int").alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("product_category"),
        F.col("supplier_name"),
        F.coalesce(F.col("supplier_city"), F.lit("")).alias("supplier_city"),
        F.when(F.col("product_price") != "", F.col("product_price").cast("double")).alias("price"),
        F.col("product_weight").alias("weight"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.when(F.col("product_rating") != "",  F.col("product_rating").cast("double")).alias("rating"),
        F.when(F.col("product_reviews") != "", F.col("product_reviews").cast("int")).alias("reviews"),
        F.when(F.col("product_release_date") != "",
               F.to_date("product_release_date", "M/d/yyyy")).alias("release_date"),
        F.when(F.col("product_expiry_date") != "",
               F.to_date("product_expiry_date",  "M/d/yyyy")).alias("expiry_date")
    )
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

# чтобы избежать коллизий: переименовываем колонки в правых DF перед join'ом
supplier_for_join = (
    dim_supplier
    .select(
        F.col("supplier_id"),
        F.col("name").alias("_sup_name"),
        F.col("city").alias("_sup_city"),
    )
)
cat_for_join = dim_cat.select("category_id", F.col("category_name").alias("_cat_name"))

dim_product = (
    dim_product_raw
    .join(cat_for_join, F.col("product_category") == F.col("_cat_name"), "left")
    .join(supplier_for_join,
          (F.col("supplier_name") == F.col("_sup_name")) &
          (F.col("supplier_city") == F.col("_sup_city")), "left")
    .select("product_id", "name", "category_id", "supplier_id",
            "price", "weight", "color", "size", "brand", "material",
            "description", "rating", "reviews", "release_date", "expiry_date")
)
dim_product.write.jdbc(PG_URL, "star.dim_product", mode="overwrite", properties=PG_PROPS)
print(f"[dim_product] записано: {dim_product.count()}")


# ---------- dim_store ----------
dim_store = (
    raw.select(
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.coalesce(F.col("store_city"), F.lit("")).alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email")
    )
    .filter(F.col("name").isNotNull() & (F.col("name") != ""))
    .dropDuplicates(["name", "city"])
    .withColumn("store_id",
                F.row_number().over(Window.orderBy("name", "city")))
    .select("store_id", "name", "location", "city", "state", "country", "phone", "email")
)
dim_store.write.jdbc(PG_URL, "star.dim_store", mode="overwrite", properties=PG_PROPS)
print(f"[dim_store] записано: {dim_store.count()}")


# ---------- dim_date ----------
dim_date = (
    raw.select(F.to_date("sale_date", "M/d/yyyy").alias("full_date"))
       .filter(F.col("full_date").isNotNull())
       .dropDuplicates()
       .withColumn("date_id",  F.row_number().over(Window.orderBy("full_date")))
       .withColumn("day",      F.dayofmonth("full_date").cast("short"))
       .withColumn("month",    F.month("full_date").cast("short"))
       .withColumn("year",     F.year("full_date").cast("short"))
       .withColumn("quarter",  F.quarter("full_date").cast("short"))
       .select("date_id", "full_date", "day", "month", "year", "quarter")
)
dim_date.write.jdbc(PG_URL, "star.dim_date", mode="overwrite", properties=PG_PROPS)
print(f"[dim_date] записано: {dim_date.count()}")


# ---------- fact_sales ----------
# mock_data.id повторяется 1-1000 в каждом CSV-файле (10 файлов),
# поэтому используем monotonically_increasing_id для уникального PK
fact_raw = (
    raw.select(
        F.monotonically_increasing_id().alias("sale_id"),
        F.to_date("sale_date", "M/d/yyyy").alias("sale_dt"),
        F.col("sale_customer_id").cast("int").alias("customer_id"),
        F.col("sale_seller_id").cast("int").alias("seller_id"),
        F.col("sale_product_id").cast("int").alias("product_id"),
        F.col("store_name"),
        F.coalesce(F.col("store_city"), F.lit("")).alias("store_city"),
        F.when(F.col("sale_quantity")    != "", F.col("sale_quantity").cast("int")).alias("quantity"),
        F.when(F.col("sale_total_price") != "", F.col("sale_total_price").cast("double")).alias("total_price")
    )
    .filter(F.col("sale_dt").isNotNull())
)

date_for_join  = dim_date.select("date_id", F.col("full_date").alias("_dt"))
store_for_join = dim_store.select(
    "store_id",
    F.col("name").alias("_st_name"),
    F.col("city").alias("_st_city"),
)

fact_sales = (
    fact_raw
    .join(date_for_join,  F.col("sale_dt") == F.col("_dt"), "left")
    .join(store_for_join,
          (F.col("store_name") == F.col("_st_name")) &
          (F.col("store_city") == F.col("_st_city")), "left")
    .select("sale_id", "date_id", "customer_id", "seller_id",
            "product_id", "store_id", "quantity", "total_price")
)
fact_sales.write.jdbc(PG_URL, "star.fact_sales", mode="overwrite", properties=PG_PROPS)
print(f"[fact_sales] записано: {fact_sales.count()}")

print("\nETL завершён успешно!")
spark.stop()
