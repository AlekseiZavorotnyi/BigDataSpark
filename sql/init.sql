DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS mock_data CASCADE;

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
    supplier_country text
    );

DO $$
DECLARE
csv_file text;
    loaded_rows bigint;
BEGIN
    FOREACH csv_file IN ARRAY ARRAY[
        'MOCK_DATA.csv',
        'MOCK_DATA (1).csv',
        'MOCK_DATA (2).csv',
        'MOCK_DATA (3).csv',
        'MOCK_DATA (4).csv',
        'MOCK_DATA (5).csv',
        'MOCK_DATA (6).csv',
        'MOCK_DATA (7).csv',
        'MOCK_DATA (8).csv',
        'MOCK_DATA (9).csv'
    ] LOOP
        EXECUTE format($copy$
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
                supplier_phone, supplier_address, supplier_city, supplier_country
            )
            FROM %L
            WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '')
        $copy$, '/source_data/' || csv_file);

GET DIAGNOSTICS loaded_rows = ROW_COUNT;
RAISE NOTICE 'Loaded % rows from %', loaded_rows, csv_file;
END LOOP;
END $$;
