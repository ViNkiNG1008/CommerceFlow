INSERT INTO gold.dim_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM public.olist_customers
ON CONFLICT (customer_id) DO NOTHING;


INSERT INTO gold.dim_products (
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM public.olist_products p
LEFT JOIN public.product_category_name_translation t
    ON p.product_category_name = t.product_category_name
ON CONFLICT (product_id) DO NOTHING;


INSERT INTO gold.dim_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM public.olist_sellers
ON CONFLICT (seller_id) DO NOTHING;


INSERT INTO gold.dim_date (
    date_key,
    full_date,
    day_of_month,
    day_name,
    day_of_week,
    week_of_year,
    month_number,
    month_name,
    quarter_number,
    year_number,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(DAY FROM d)::INTEGER,
    TO_CHAR(d, 'FMDay'),
    EXTRACT(ISODOW FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'FMMonth'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM GENERATE_SERIES(
    DATE '2016-09-04',
    DATE '2018-10-17',
    INTERVAL '1 DAY'
) AS dates(d)
ON CONFLICT (date_key) DO NOTHING;


