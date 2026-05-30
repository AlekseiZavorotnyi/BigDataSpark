DROP TABLE IF EXISTS mock_data CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS fact_sales CASCADE;

CREATE TABLE IF NOT EXISTS mock_data (
                                         id integer,
                                         customer_first_name text,
                                         customer_last_name text,
                                         customer_age integer,
                                         customer_email text,
                                         customer_country text,
                                         customer_postal_code text,
                                         customer_pet_type text,
                                         customer_pet_name text,
                                         customer_pet_breed text,
                                         seller_first_name text,
                                         seller_last_name text,
                                         seller_email text,
                                         seller_country text,
                                         seller_postal_code text,
                                         product_name text,
                                         product_category text,
                                         product_price numeric(12, 2),
    product_quantity integer,
    sale_date date,
    sale_customer_id integer,
    sale_seller_id integer,
    sale_product_id integer,
    sale_quantity integer,
    sale_total_price numeric(14, 2),
    store_name text,
    store_location text,
    store_city text,
    store_state text,
    store_country text,
    store_phone text,
    store_email text,
    pet_category text,
    product_weight numeric(12, 2),
    product_color text,
    product_size text,
    product_brand text,
    product_material text,
    product_description text,
    product_rating numeric(4, 2),
    product_reviews integer,
    product_release_date date,
    product_expiry_date date,
    supplier_name text,
    supplier_contact text,
    supplier_email text,
    supplier_phone text,
    supplier_address text,
    supplier_city text,
    supplier_country text,
    source_file text
    );

DO $$
DECLARE
file_name text;
    file_path text;
    i integer;
BEGIN
FOR i IN 1..10 LOOP
        file_path := '/source_data/mock_data_' || i || '.csv';
BEGIN
EXECUTE format('
    COPY mock_data(
    id, customer_first_name, customer_last_name, customer_age,
    customer_email, customer_country, customer_postal_code,
    customer_pet_type, customer_pet_name, customer_pet_breed,
    seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code,
    product_name, product_category, product_price, product_quantity,
    sale_date, sale_customer_id, sale_seller_id, sale_product_id,
    sale_quantity, sale_total_price,
    store_name, store_location, store_city, store_state,
    store_country, store_phone, store_email,
    pet_category, product_weight, product_color, product_size,
    product_brand, product_material, product_description,
    product_rating, product_reviews, product_release_date,
    product_expiry_date,
    supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country,
    source_file
    ) FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')
    ', file_path);
RAISE NOTICE 'Loaded % successfully', file_path;
EXCEPTION WHEN OTHERS THEN
RAISE NOTICE 'Failed to load %: %', file_path, SQLERRM;
END;
END LOOP;
END $$;

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT UNIQUE,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT
);

CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT UNIQUE,
    seller_country TEXT,
    seller_postal_code TEXT
);

CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT UNIQUE,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
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
);

CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

CREATE TABLE fact_sales (
    sale_key SERIAL PRIMARY KEY,
    customer_key INT REFERENCES dim_customer(customer_key),
    seller_key INT REFERENCES dim_seller(seller_key),
    product_key INT REFERENCES dim_product(product_key),
    store_key INT REFERENCES dim_store(store_key),
    supplier_key INT REFERENCES dim_supplier(supplier_key),
    date_key INT REFERENCES dim_date(date_key),
    quantity INT,
    total_price NUMERIC(14,2),
    unit_price NUMERIC(12,2),
    product_rating NUMERIC(4,2),
    product_reviews INT,
    stock_quantity INT,
    product_release_date DATE,
    product_expiry_date DATE
);