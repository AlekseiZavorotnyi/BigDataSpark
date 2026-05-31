from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark, execute_jdbc_statements, load_config, postgres_properties


def create_dimension(df, columns, key_columns, key_name):
    order_columns = [F.col(column).asc_nulls_last() for column in key_columns]
    window = Window.orderBy(*order_columns)

    return (
        df.select(*columns)
        .dropDuplicates(key_columns)
        .withColumn(key_name, F.row_number().over(window))
        .select(key_name, *columns)
    )


def null_safe_join(left, right, pairs):
    condition = None
    for left_column, right_column in pairs:
        part = F.col(left_column).eqNullSafe(F.col(right_column))
        condition = part if condition is None else condition & part
    return left.join(right, condition)


def recreate_star_schema(spark, pg):
    execute_jdbc_statements(
        spark=spark,
        url=pg["url"],
        user=pg["user"],
        password=pg["password"],
        driver=pg["driver"],
        statements=[
            "DROP TABLE IF EXISTS fact_sales",
            "DROP TABLE IF EXISTS dim_customer",
            "DROP TABLE IF EXISTS dim_seller",
            "DROP TABLE IF EXISTS dim_store",
            "DROP TABLE IF EXISTS dim_supplier",
            "DROP TABLE IF EXISTS dim_product",
            "DROP TABLE IF EXISTS dim_date",
            """
            CREATE TABLE dim_customer (
                customer_key INT PRIMARY KEY,
                customer_first_name TEXT,
                customer_last_name TEXT,
                customer_age INT,
                customer_email TEXT,
                customer_country TEXT,
                customer_postal_code TEXT,
                customer_pet_type TEXT,
                customer_pet_name TEXT,
                customer_pet_breed TEXT
            )
            """,
            """
            CREATE TABLE dim_seller (
                seller_key INT PRIMARY KEY,
                seller_first_name TEXT,
                seller_last_name TEXT,
                seller_email TEXT,
                seller_country TEXT,
                seller_postal_code TEXT
            )
            """,
            """
            CREATE TABLE dim_store (
                store_key INT PRIMARY KEY,
                store_name TEXT,
                store_location TEXT,
                store_city TEXT,
                store_state TEXT,
                store_country TEXT,
                store_phone TEXT,
                store_email TEXT
            )
            """,
            """
            CREATE TABLE dim_supplier (
                supplier_key INT PRIMARY KEY,
                supplier_name TEXT,
                supplier_contact TEXT,
                supplier_email TEXT,
                supplier_phone TEXT,
                supplier_address TEXT,
                supplier_city TEXT,
                supplier_country TEXT
            )
            """,
            """
            CREATE TABLE dim_product (
                product_key INT PRIMARY KEY,
                product_name TEXT,
                product_category TEXT,
                product_brand TEXT,
                product_color TEXT,
                product_size TEXT,
                product_material TEXT,
                product_weight NUMERIC(12,2),
                product_description TEXT,
                pet_category TEXT,
                supplier_name TEXT,
                supplier_email TEXT
            )
            """,
            """
            CREATE TABLE dim_date (
                date_key INT PRIMARY KEY,
                full_date DATE,
                year INT,
                month INT,
                day INT
            )
            """,
            """
            CREATE TABLE fact_sales (
                sale_key BIGINT PRIMARY KEY,
                customer_key INT,
                seller_key INT,
                product_key INT,
                store_key INT,
                supplier_key INT,
                date_key INT,
                quantity INT,
                total_price NUMERIC(14,2),
                unit_price NUMERIC(12,2),
                product_rating NUMERIC(4,2),
                product_reviews INT,
                stock_quantity INT,
                product_release_date DATE,
                product_expiry_date DATE
            )
            """,
        ],
    )


def write_postgres(df, pg_url, table, properties):
    df.write.mode("append").jdbc(url=pg_url, table=table, properties=properties)
    print(f"{table}: {df.count()} rows")


def main():
    config = load_config()
    pg = config["postgres"]
    pg_url = pg["url"]
    pg_props = postgres_properties(config)

    spark = create_spark("BigDataSpark-Star-ETL")
    spark.sparkContext.setLogLevel("WARN")

    print("Reading mock_data from PostgreSQL")
    raw = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_props)
    df = raw.withColumn("sale_date", F.to_date("sale_date"))
    raw_count = df.count()
    print(f"mock_data: {raw_count} rows")

    recreate_star_schema(spark, pg)

    dim_customer = create_dimension(
        df,
        [
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
        ],
        ["customer_email"],
        "customer_key",
    )

    dim_seller = create_dimension(
        df,
        ["seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"],
        ["seller_email"],
        "seller_key",
    )

    dim_store = create_dimension(
        df,
        ["store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"],
        ["store_name", "store_city", "store_country"],
        "store_key",
    )

    dim_supplier = create_dimension(
        df,
        [
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        ],
        ["supplier_name", "supplier_email"],
        "supplier_key",
    )

    dim_product = create_dimension(
        df,
        [
            "product_name",
            "product_category",
            "product_brand",
            "product_color",
            "product_size",
            "product_material",
            "product_weight",
            "product_description",
            "pet_category",
            "supplier_name",
            "supplier_email",
        ],
        ["product_name", "product_brand", "product_size", "product_color"],
        "product_key",
    )

    dim_date = (
        df.select(F.col("sale_date").alias("full_date"))
        .filter(F.col("full_date").isNotNull())
        .dropDuplicates(["full_date"])
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("date_key", F.row_number().over(Window.orderBy("full_date")))
        .select("date_key", "full_date", "year", "month", "day")
    )

    print("Writing dimensions to PostgreSQL")
    write_postgres(dim_customer, pg_url, "dim_customer", pg_props)
    write_postgres(dim_seller, pg_url, "dim_seller", pg_props)
    write_postgres(dim_store, pg_url, "dim_store", pg_props)
    write_postgres(dim_supplier, pg_url, "dim_supplier", pg_props)
    write_postgres(dim_product, pg_url, "dim_product", pg_props)
    write_postgres(dim_date, pg_url, "dim_date", pg_props)

    print("Building fact_sales")
    fact_source = df.alias("m")
    fact = null_safe_join(fact_source, dim_customer.alias("dc"), [("m.customer_email", "dc.customer_email")])
    fact = null_safe_join(fact, dim_seller.alias("ds"), [("m.seller_email", "ds.seller_email")])
    fact = null_safe_join(
        fact,
        dim_store.alias("st"),
        [
            ("m.store_name", "st.store_name"),
            ("m.store_city", "st.store_city"),
            ("m.store_country", "st.store_country"),
        ],
    )
    fact = null_safe_join(
        fact,
        dim_supplier.alias("sp"),
        [("m.supplier_name", "sp.supplier_name"), ("m.supplier_email", "sp.supplier_email")],
    )
    fact = null_safe_join(
        fact,
        dim_product.alias("dp"),
        [
            ("m.product_name", "dp.product_name"),
            ("m.product_brand", "dp.product_brand"),
            ("m.product_size", "dp.product_size"),
            ("m.product_color", "dp.product_color"),
        ],
    )
    fact = null_safe_join(fact, dim_date.alias("dd"), [("m.sale_date", "dd.full_date")])

    fact_selected = (
        fact.select(
            F.col("dc.customer_key").alias("customer_key"),
            F.col("ds.seller_key").alias("seller_key"),
            F.col("dp.product_key").alias("product_key"),
            F.col("st.store_key").alias("store_key"),
            F.col("sp.supplier_key").alias("supplier_key"),
            F.col("dd.date_key").alias("date_key"),
            F.col("m.sale_quantity").cast("int").alias("quantity"),
            F.col("m.sale_total_price").cast("decimal(14,2)").alias("total_price"),
            F.col("m.product_price").cast("decimal(12,2)").alias("unit_price"),
            F.col("m.product_rating").cast("decimal(4,2)").alias("product_rating"),
            F.col("m.product_reviews").cast("int").alias("product_reviews"),
            F.col("m.product_quantity").cast("int").alias("stock_quantity"),
            F.to_date(F.col("m.product_release_date")).alias("product_release_date"),
            F.to_date(F.col("m.product_expiry_date")).alias("product_expiry_date"),
        )
        .withColumn(
            "sale_key",
            F.row_number().over(
                Window.orderBy(
                    "date_key",
                    "customer_key",
                    "seller_key",
                    "product_key",
                    "store_key",
                    "supplier_key",
                    "total_price",
                )
            ),
        )
        .select(
            "sale_key",
            "customer_key",
            "seller_key",
            "product_key",
            "store_key",
            "supplier_key",
            "date_key",
            "quantity",
            "total_price",
            "unit_price",
            "product_rating",
            "product_reviews",
            "stock_quantity",
            "product_release_date",
            "product_expiry_date",
        )
    )

    write_postgres(fact_selected, pg_url, "fact_sales", pg_props)
    fact_count = fact_selected.count()

    if fact_count != raw_count:
        raise RuntimeError(f"fact_sales row count mismatch: expected {raw_count}, got {fact_count}")

    print("Star ETL completed successfully")


if __name__ == "__main__":
    main()
