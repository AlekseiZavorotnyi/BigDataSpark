from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    create_spark,
    load_config,
    postgres_properties,
    recreate_clickhouse_table,
    write_clickhouse_json,
)


REPORT_SCHEMAS = {
    "report_product_sales": """
        CREATE TABLE report_product_sales (
            product_key Nullable(Int32),
            product_name Nullable(String),
            product_category Nullable(String),
            total_sold Nullable(Int64),
            total_revenue Nullable(Float64),
            category_revenue Nullable(Float64),
            avg_rating Nullable(Float64),
            total_reviews Nullable(Int64),
            orders_count Nullable(Int64),
            sales_rank Nullable(Int32)
        ) ENGINE = MergeTree ORDER BY tuple()
    """,
    "report_customer_sales": """
        CREATE TABLE report_customer_sales (
            customer_key Nullable(Int32),
            customer_first_name Nullable(String),
            customer_last_name Nullable(String),
            customer_email Nullable(String),
            customer_country Nullable(String),
            total_spent Nullable(Float64),
            avg_check Nullable(Float64),
            orders_count Nullable(Int64),
            country_customer_count Nullable(Int64),
            spend_rank Nullable(Int32)
        ) ENGINE = MergeTree ORDER BY tuple()
    """,
    "report_time_sales": """
        CREATE TABLE report_time_sales (
            year Nullable(Int32),
            month Nullable(Int32),
            total_revenue Nullable(Float64),
            orders_count Nullable(Int64),
            items_sold Nullable(Int64),
            avg_order_value Nullable(Float64),
            previous_month_revenue Nullable(Float64),
            revenue_delta Nullable(Float64)
        ) ENGINE = MergeTree ORDER BY tuple()
    """,
    "report_store_sales": """
        CREATE TABLE report_store_sales (
            store_key Nullable(Int32),
            store_name Nullable(String),
            store_city Nullable(String),
            store_country Nullable(String),
            total_revenue Nullable(Float64),
            avg_check Nullable(Float64),
            orders_count Nullable(Int64),
            location_revenue Nullable(Float64),
            revenue_rank Nullable(Int32)
        ) ENGINE = MergeTree ORDER BY tuple()
    """,
    "report_supplier_sales": """
        CREATE TABLE report_supplier_sales (
            supplier_key Nullable(Int32),
            supplier_name Nullable(String),
            supplier_country Nullable(String),
            total_revenue Nullable(Float64),
            avg_unit_price Nullable(Float64),
            unique_products Nullable(Int64),
            orders_count Nullable(Int64),
            items_sold Nullable(Int64),
            country_revenue Nullable(Float64),
            revenue_rank Nullable(Int32)
        ) ENGINE = MergeTree ORDER BY tuple()
    """,
    "report_product_quality": """
        CREATE TABLE report_product_quality (
            product_key Nullable(Int32),
            product_name Nullable(String),
            product_category Nullable(String),
            avg_rating Nullable(Float64),
            total_sold Nullable(Int64),
            total_revenue Nullable(Float64),
            total_reviews Nullable(Int64),
            rating_rank_desc Nullable(Int32),
            rating_rank_asc Nullable(Int32),
            review_rank Nullable(Int32),
            rating_sales_corr Nullable(Float64)
        ) ENGINE = MergeTree ORDER BY tuple()
    """,
}


def read_pg(spark, pg_url, pg_props, table):
    return spark.read.jdbc(url=pg_url, table=table, properties=pg_props)


def write_report(ch_config, name, dataframe):
    print(f"Writing {name}")
    recreate_clickhouse_table(ch_config, name, REPORT_SCHEMAS[name])
    write_clickhouse_json(ch_config, name, dataframe)
    print(f"{name}: {dataframe.count()} rows")


def main():
    config = load_config()
    pg = config["postgres"]
    ch = config["clickhouse"]
    pg_url = pg["url"]
    pg_props = postgres_properties(config)

    spark = create_spark("BigDataSpark-ClickHouse-Reports")
    spark.sparkContext.setLogLevel("WARN")

    print("Reading star schema from PostgreSQL")
    fact = read_pg(spark, pg_url, pg_props, "fact_sales").alias("f")
    dim_customer = read_pg(spark, pg_url, pg_props, "dim_customer").alias("dc")
    dim_product = read_pg(spark, pg_url, pg_props, "dim_product").alias("dp")
    dim_store = read_pg(spark, pg_url, pg_props, "dim_store").alias("dst")
    dim_supplier = read_pg(spark, pg_url, pg_props, "dim_supplier").alias("dsp")
    dim_date = read_pg(spark, pg_url, pg_props, "dim_date").alias("dd")

    product_base = (
        fact.join(dim_product, "product_key")
        .groupBy("product_key", "product_name", "product_category")
        .agg(
            F.sum("quantity").cast("long").alias("total_sold"),
            F.sum("total_price").cast("double").alias("total_revenue"),
            F.avg("product_rating").cast("double").alias("avg_rating"),
            F.sum("product_reviews").cast("long").alias("total_reviews"),
            F.count("*").cast("long").alias("orders_count"),
        )
    )
    category_revenue = product_base.groupBy("product_category").agg(
        F.sum("total_revenue").cast("double").alias("category_revenue")
    )
    product_report = (
        product_base.join(category_revenue, "product_category")
        .withColumn("sales_rank", F.row_number().over(Window.orderBy(F.desc("total_sold"))))
        .select(
            "product_key",
            "product_name",
            "product_category",
            "total_sold",
            "total_revenue",
            "category_revenue",
            "avg_rating",
            "total_reviews",
            "orders_count",
            "sales_rank",
        )
    )

    customer_countries = dim_customer.groupBy("customer_country").agg(
        F.count("*").cast("long").alias("country_customer_count")
    )
    customer_report = (
        fact.join(dim_customer, "customer_key")
        .groupBy(
            "customer_key",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.sum("total_price").cast("double").alias("total_spent"),
            F.avg("total_price").cast("double").alias("avg_check"),
            F.count("*").cast("long").alias("orders_count"),
        )
        .join(customer_countries, "customer_country")
        .withColumn("spend_rank", F.row_number().over(Window.orderBy(F.desc("total_spent"))))
        .select(
            "customer_key",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
            "total_spent",
            "avg_check",
            "orders_count",
            "country_customer_count",
            "spend_rank",
        )
    )

    time_window = Window.orderBy("year", "month")
    time_report = (
        fact.join(dim_date, "date_key")
        .groupBy("year", "month")
        .agg(
            F.sum("total_price").cast("double").alias("total_revenue"),
            F.count("*").cast("long").alias("orders_count"),
            F.sum("quantity").cast("long").alias("items_sold"),
            F.avg("total_price").cast("double").alias("avg_order_value"),
        )
        .withColumn("previous_month_revenue", F.lag("total_revenue").over(time_window))
        .withColumn("revenue_delta", F.col("total_revenue") - F.col("previous_month_revenue"))
        .select(
            "year",
            "month",
            "total_revenue",
            "orders_count",
            "items_sold",
            "avg_order_value",
            "previous_month_revenue",
            "revenue_delta",
        )
    )

    location_revenue = (
        fact.join(dim_store, "store_key")
        .groupBy("store_country", "store_city")
        .agg(F.sum("total_price").cast("double").alias("location_revenue"))
    )
    store_report = (
        fact.join(dim_store, "store_key")
        .groupBy("store_key", "store_name", "store_city", "store_country")
        .agg(
            F.sum("total_price").cast("double").alias("total_revenue"),
            F.avg("total_price").cast("double").alias("avg_check"),
            F.count("*").cast("long").alias("orders_count"),
        )
        .join(location_revenue, ["store_country", "store_city"])
        .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
        .select(
            "store_key",
            "store_name",
            "store_city",
            "store_country",
            "total_revenue",
            "avg_check",
            "orders_count",
            "location_revenue",
            "revenue_rank",
        )
    )

    supplier_country_revenue = (
        fact.join(dim_supplier, "supplier_key")
        .groupBy("supplier_country")
        .agg(F.sum("total_price").cast("double").alias("country_revenue"))
    )
    supplier_report = (
        fact.join(dim_supplier, "supplier_key")
        .groupBy("supplier_key", "supplier_name", "supplier_country")
        .agg(
            F.sum("total_price").cast("double").alias("total_revenue"),
            F.avg("unit_price").cast("double").alias("avg_unit_price"),
            F.countDistinct("product_key").cast("long").alias("unique_products"),
            F.count("*").cast("long").alias("orders_count"),
            F.sum("quantity").cast("long").alias("items_sold"),
        )
        .join(supplier_country_revenue, "supplier_country")
        .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
        .select(
            "supplier_key",
            "supplier_name",
            "supplier_country",
            "total_revenue",
            "avg_unit_price",
            "unique_products",
            "orders_count",
            "items_sold",
            "country_revenue",
            "revenue_rank",
        )
    )

    quality_base = (
        fact.join(dim_product, "product_key")
        .groupBy("product_key", "product_name", "product_category")
        .agg(
            F.avg("product_rating").cast("double").alias("avg_rating"),
            F.sum("quantity").cast("long").alias("total_sold"),
            F.sum("total_price").cast("double").alias("total_revenue"),
            F.sum("product_reviews").cast("long").alias("total_reviews"),
        )
    )
    corr_row = quality_base.select(F.corr("avg_rating", "total_sold").alias("corr")).first()
    rating_sales_corr = corr_row["corr"] if corr_row and corr_row["corr"] is not None else None
    quality_report = (
        quality_base.withColumn("rating_rank_desc", F.row_number().over(Window.orderBy(F.desc("avg_rating"))))
        .withColumn("rating_rank_asc", F.row_number().over(Window.orderBy(F.asc("avg_rating"))))
        .withColumn("review_rank", F.row_number().over(Window.orderBy(F.desc("total_reviews"))))
        .withColumn("rating_sales_corr", F.lit(rating_sales_corr).cast("double"))
        .select(
            "product_key",
            "product_name",
            "product_category",
            "avg_rating",
            "total_sold",
            "total_revenue",
            "total_reviews",
            "rating_rank_desc",
            "rating_rank_asc",
            "review_rank",
            "rating_sales_corr",
        )
    )

    write_report(ch, "report_product_sales", product_report)
    write_report(ch, "report_customer_sales", customer_report)
    write_report(ch, "report_time_sales", time_report)
    write_report(ch, "report_store_sales", store_report)
    write_report(ch, "report_supplier_sales", supplier_report)
    write_report(ch, "report_product_quality", quality_report)

    print("ClickHouse reports completed successfully")


if __name__ == "__main__":
    main()
