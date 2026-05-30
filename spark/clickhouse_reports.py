import yaml
import subprocess
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def setup_jdbc_drivers():
    """Установка JDBC драйверов программно"""
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
    .appName("ClickHouseReports")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.jars", f"{jars_dir}/postgresql-42.7.1.jar,{jars_dir}/clickhouse-jdbc-0.6.3-shaded.jar")
    .getOrCreate()
)

pg = cfg["postgres"]
ch = cfg["clickhouse"]

pg_url = pg["url"]
pg_properties = {
    "user": pg["user"],
    "password": pg["password"],
    "driver": pg["driver"]
}

ch_url = ch["url"]
ch_properties = {
    "user": ch["user"],
    "password": ch["password"],
    "driver": ch["driver"]
}

def create_clickhouse_table(table_name, schema_sql):
    try:
        from jaydebeapi import connect
        import jpype

        conn = connect(
            "com.clickhouse.jdbc.ClickHouseDriver",
            ch_url,
            {"user": ch["user"], "password": ch["password"]},
            f"{jars_dir}/clickhouse-jdbc-0.6.3-shaded.jar"
        )
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(schema_sql)
        conn.close()
        print(f"  Table {table_name} created")
    except Exception as e:
        print(f"  Warning: Could not create table {table_name}: {e}")

print("Reading data from PostgreSQL...")
fact = spark.read.jdbc(url=pg_url, table="fact_sales", properties=pg_properties)
dim_customer = spark.read.jdbc(url=pg_url, table="dim_customer", properties=pg_properties)
dim_product = spark.read.jdbc(url=pg_url, table="dim_product", properties=pg_properties)
dim_store = spark.read.jdbc(url=pg_url, table="dim_store", properties=pg_properties)
dim_supplier = spark.read.jdbc(url=pg_url, table="dim_supplier", properties=pg_properties)
dim_date = spark.read.jdbc(url=pg_url, table="dim_date", properties=pg_properties)

print("Data loaded successfully!")

f = fact.alias("f")
dp = dim_product.alias("dp")
dc = dim_customer.alias("dc")
dst = dim_store.alias("dst")
dsp = dim_supplier.alias("dsp")
dd = dim_date.alias("dd")


print("Creating top10_products...")
top10_products = (
    f.join(dp, "product_key")
    .groupBy("product_key", "product_name")
    .agg(F.sum("quantity").alias("total_sold"))
    .orderBy(F.desc("total_sold"))
    .limit(10)
)
top10_products.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_top10_products", properties=ch_properties)

print("Creating revenue_by_category...")
revenue_by_category = (
    f.join(dp, "product_key")
    .groupBy("product_category")
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("sales_count")
    )
    .orderBy(F.desc("total_revenue"))
)
revenue_by_category.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_revenue_by_category", properties=ch_properties)

print("Creating product_ratings...")
product_ratings = (
    f.join(dp, "product_key")
    .groupBy("product_key", "product_name", "product_category")
    .agg(
        F.avg(F.col("product_rating")).alias("avg_rating"),
        F.sum(F.col("product_reviews")).alias("total_reviews"),
        F.sum("quantity").alias("total_sold")
    )
    .orderBy(F.desc("avg_rating"))
)
product_ratings.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_product_ratings", properties=ch_properties)


print("Creating top10_customers...")
top10_customers = (
    f.join(dc, "customer_key")
    .groupBy("customer_key", "customer_email", "customer_country")
    .agg(F.sum("total_price").alias("total_spent"))
    .orderBy(F.desc("total_spent"))
    .limit(10)
)
top10_customers.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_top10_customers", properties=ch_properties)

print("Creating customers_by_country...")
customers_by_country = (
    dc.groupBy("customer_country")
    .agg(F.count("*").alias("customer_count"))
    .orderBy(F.desc("customer_count"))
)
customers_by_country.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_customers_by_country", properties=ch_properties)

print("Creating avg_check_by_customer...")
avg_check_by_customer = (
    f.join(dc, "customer_key")
    .groupBy("customer_key", "customer_email", "customer_country")
    .agg(
        F.avg("total_price").alias("avg_check"),
        F.count("*").alias("orders_count"),
        F.sum("total_price").alias("total_spent")
    )
    .orderBy(F.desc("avg_check"))
)
avg_check_by_customer.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_avg_check_by_customer", properties=ch_properties)


print("Creating monthly_yearly_trends...")
monthly_yearly_trends = (
    f.join(dd, "date_key")
    .groupBy("year", "month")
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("orders_count"),
        F.sum("quantity").alias("items_sold")
    )
    .orderBy("year", "month")
)
monthly_yearly_trends.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_monthly_trends", properties=ch_properties)

print("Creating yearly_comparison...")
yearly_comparison = (
    f.join(dd, "date_key")
    .groupBy("year")
    .agg(
        F.sum("total_price").alias("yearly_revenue"),
        F.count("*").alias("yearly_orders")
    )
    .orderBy("year")
)
yearly_comparison.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_yearly_comparison", properties=ch_properties)

print("Creating avg_order_by_month...")
avg_order_by_month = (
    f.join(dd, "date_key")
    .groupBy("year", "month")
    .agg(
        F.avg("total_price").alias("avg_order_value"),
        F.avg("quantity").alias("avg_items_per_order")
    )
    .orderBy("year", "month")
)
avg_order_by_month.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_avg_order_by_month", properties=ch_properties)


print("Creating top5_stores...")
top5_stores = (
    f.join(dst, "store_key")
    .groupBy("store_key", "store_name", "store_city", "store_country")
    .agg(F.sum("total_price").alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
    .limit(5)
)
top5_stores.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_top5_stores", properties=ch_properties)

print("Creating sales_by_location...")
sales_by_location = (
    f.join(dst, "store_key")
    .groupBy("store_country", "store_city")
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("orders_count")
    )
    .orderBy(F.desc("total_revenue"))
)
sales_by_location.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_sales_by_location", properties=ch_properties)

print("Creating avg_check_by_store...")
avg_check_by_store = (
    f.join(dst, "store_key")
    .groupBy("store_key", "store_name", "store_city", "store_country")
    .agg(
        F.avg("total_price").alias("avg_check"),
        F.count("*").alias("orders_count"),
        F.sum("total_price").alias("total_revenue")
    )
    .orderBy(F.desc("avg_check"))
)
avg_check_by_store.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_avg_check_by_store", properties=ch_properties)


print("Creating top5_suppliers...")
top5_suppliers = (
    f.join(dsp, "supplier_key")
    .groupBy("supplier_key", "supplier_name", "supplier_country")
    .agg(F.sum("total_price").alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
    .limit(5)
)
top5_suppliers.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_top5_suppliers", properties=ch_properties)

print("Creating avg_price_by_supplier...")
avg_price_by_supplier = (
    f.join(dsp, "supplier_key")
    .join(dp, "product_key")
    .groupBy("supplier_key", "supplier_name", "supplier_country")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("product_key").alias("unique_products")
    )
    .orderBy(F.desc("avg_unit_price"))
)
avg_price_by_supplier.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_avg_price_by_supplier", properties=ch_properties)

print("Creating sales_by_supplier_country...")
sales_by_supplier_country = (
    f.join(dsp, "supplier_key")
    .groupBy("supplier_country")
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("orders_count"),
        F.sum("quantity").alias("items_sold")
    )
    .orderBy(F.desc("total_revenue"))
)
sales_by_supplier_country.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_sales_by_supplier_country", properties=ch_properties)


print("Creating highest_rated...")
highest_rated = (
    f.join(dp, "product_key")
    .groupBy("product_key", "product_name", "product_category")
    .agg(F.avg("product_rating").alias("avg_rating"))
    .orderBy(F.desc("avg_rating"))
    .limit(10)
)
highest_rated.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_highest_rated", properties=ch_properties)

print("Creating lowest_rated...")
lowest_rated = (
    f.join(dp, "product_key")
    .groupBy("product_key", "product_name", "product_category")
    .agg(F.avg("product_rating").alias("avg_rating"))
    .orderBy(F.asc("avg_rating"))
    .limit(10)
)
lowest_rated.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_lowest_rated", properties=ch_properties)

print("Creating rating_vs_sales...")
rating_vs_sales = (
    f.join(dp, "product_key")
    .groupBy("product_key", "product_name", "product_category")
    .agg(
        F.avg("product_rating").alias("avg_rating"),
        F.sum("quantity").alias("total_sold"),
        F.sum("total_price").alias("total_revenue")
    )
    .orderBy("avg_rating")
)
rating_vs_sales.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_rating_vs_sales", properties=ch_properties)

print("Creating most_reviewed...")
most_reviewed = (
    f.join(dp, "product_key")
    .groupBy("product_key", "product_name", "product_category")
    .agg(
        F.sum("product_reviews").alias("total_reviews"),
        F.avg("product_rating").alias("avg_rating")
    )
    .orderBy(F.desc("total_reviews"))
    .limit(10)
)
most_reviewed.write.mode("overwrite").option("truncate", "true").jdbc(url=ch_url, table="ch_most_reviewed", properties=ch_properties)

print("=" * 60)
print("All ClickHouse reports generated successfully!")
print(f"Total tables created: 18 reports covering all 6 requirements")
print("=" * 60)