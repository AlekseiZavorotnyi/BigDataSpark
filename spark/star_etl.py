import yaml
import subprocess
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def setup_jdbc_drivers():
    jars_dir = "/opt/bitnami/spark/jars"
    if not os.path.exists(jars_dir):
        jars_dir = "/opt/spark/jars"
    if not os.path.exists(jars_dir):
        jars_dir = "./jars"
        os.makedirs(jars_dir, exist_ok=True)

    pg_jar = os.path.join(jars_dir, "postgresql-42.7.1.jar")
    if not os.path.exists(pg_jar):
        print("Downloading PostgreSQL JDBC driver...")
        subprocess.run([
            "curl", "-L", "-o", pg_jar,
            "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar"
        ], check=True)

    ch_jar = os.path.join(jars_dir, "clickhouse-jdbc-0.6.3-shaded.jar")
    if not os.path.exists(ch_jar):
        print("Downloading ClickHouse JDBC driver...")
        subprocess.run([
            "curl", "-L", "-o", ch_jar,
            "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.3/clickhouse-jdbc-0.6.3-shaded.jar"
        ], check=True)

    return jars_dir

jars_dir = setup_jdbc_drivers()

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

spark = (
    SparkSession.builder
    .appName("BigDataSpark-Lab2")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.jars", f"{jars_dir}/postgresql-42.7.1.jar,{jars_dir}/clickhouse-jdbc-0.6.3-shaded.jar")
    .getOrCreate()
)

pg_url = cfg["postgres"]["url"]
pg_properties = {
    "user": cfg["postgres"]["user"],
    "password": cfg["postgres"]["password"],
    "driver": cfg["postgres"]["driver"]
}

print("Reading mock_data from PostgreSQL...")
df_raw = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_properties)

print(f"Total rows loaded: {df_raw.count()}")

df = df_raw.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))

print("Creating dimensions...")

def create_dimension(df, cols, key_cols, name):
    dim = df.select(*cols).dropDuplicates(key_cols)
    w = Window.orderBy(F.monotonically_increasing_id())
    dim = dim.withColumn(f"{name}_key", F.row_number().over(w))
    print(f"  {name}: {dim.count()} rows")
    return dim

dim_customer = create_dimension(df,
    ["customer_first_name", "customer_last_name", "customer_age", "customer_email",
     "customer_country", "customer_postal_code", "customer_pet_type", "customer_pet_name", "customer_pet_breed"],
    ["customer_email"], "customer")

dim_seller = create_dimension(df,
    ["seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"],
    ["seller_email"], "seller")

dim_store = create_dimension(df,
    ["store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"],
    ["store_name", "store_city", "store_country"], "store")

dim_supplier = create_dimension(df,
    ["supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address",
     "supplier_city", "supplier_country"],
    ["supplier_name", "supplier_email"], "supplier")

dim_product = create_dimension(df,
    ["product_name", "product_category", "product_brand", "product_color", "product_size",
     "product_material", "product_weight", "product_description", "pet_category", "supplier_name", "supplier_email"],
    ["product_name", "product_brand", "product_size", "product_color"], "product")

dim_date = (
    df.select("sale_date")
    .withColumnRenamed("sale_date", "full_date")
    .filter(F.col("full_date").isNotNull())
    .dropDuplicates(["full_date"])
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("date_key", F.row_number().over(Window.orderBy("full_date")))
)
print(f"  date: {dim_date.count()} rows")

print("Writing dimensions to PostgreSQL...")
for dim, name in [(dim_customer, "dim_customer"), (dim_seller, "dim_seller"),
                  (dim_store, "dim_store"), (dim_supplier, "dim_supplier"),
                  (dim_product, "dim_product"), (dim_date, "dim_date")]:
    dim.write.mode("overwrite").jdbc(url=pg_url, table=name, properties=pg_properties)
    print(f"  {name} written")

print("Reading dimensions back...")
dimensions = {}
for name in ["dim_customer", "dim_seller", "dim_store", "dim_supplier", "dim_product", "dim_date"]:
    dimensions[name] = spark.read.jdbc(url=pg_url, table=name, properties=pg_properties)

print("Creating fact table...")

from pyspark.sql.functions import broadcast

fact = df.alias("m") \
    .join(broadcast(dimensions["dim_customer"]).alias("dc"), df.customer_email == dimensions["dim_customer"].customer_email) \
    .join(broadcast(dimensions["dim_seller"]).alias("ds"), df.seller_email == dimensions["dim_seller"].seller_email) \
    .join(broadcast(dimensions["dim_store"]).alias("st"), df.store_name == dimensions["dim_store"].store_name) \
    .join(broadcast(dimensions["dim_supplier"]).alias("sp"), df.supplier_email == dimensions["dim_supplier"].supplier_email) \
    .join(broadcast(dimensions["dim_product"]).alias("dp"),
          (df.product_name == dimensions["dim_product"].product_name) &
          (df.product_brand == dimensions["dim_product"].product_brand)) \
    .join(broadcast(dimensions["dim_date"]).alias("dd"), df.sale_date == dimensions["dim_date"].full_date)

fact_selected = fact.select(
    F.col("dc.customer_key"),
    F.col("ds.seller_key"),
    F.col("dp.product_key"),
    F.col("st.store_key"),
    F.col("sp.supplier_key"),
    F.col("dd.date_key"),
    F.col("m.sale_quantity").cast("int").alias("quantity"),
    F.col("m.sale_total_price").cast("decimal(14,2)").alias("total_price"),
    F.col("m.product_price").cast("decimal(12,2)").alias("unit_price"),
    F.col("m.product_rating").cast("decimal(4,2)").alias("product_rating"),
    F.col("m.product_reviews").cast("int").alias("product_reviews"),
    F.col("m.product_quantity").cast("int").alias("stock_quantity"),
    F.to_date("product_release_date", "yyyy-MM-dd").alias("product_release_date"),
    F.to_date("product_expiry_date", "yyyy-MM-dd").alias("product_expiry_date"),
)

fact_selected.write.mode("overwrite").jdbc(url=pg_url, table="fact_sales", properties=pg_properties)

print("=" * 60)
print("ETL completed successfully!")
print(f"Fact rows: {fact_selected.count()}")
print("=" * 60)