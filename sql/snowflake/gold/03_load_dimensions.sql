
-- =====================================================
-- CommerceFlow: Snowflake Gold Dimension Loading
-- =====================================================

USE DATABASE COMMERCEFLOW_DB;
USE SCHEMA GOLD;


-- 1. Load Customer Dimension
INSERT INTO DIM_CUSTOMERS (
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ZIP_CODE_PREFIX,
    CUSTOMER_CITY,
    CUSTOMER_STATE
)
SELECT
    s."customer_id",
    s."customer_unique_id",
    s."customer_zip_code_prefix",
    s."customer_city",
    s."customer_state"
FROM SILVER.OLIST_CUSTOMERS s
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_CUSTOMERS d
    WHERE d.CUSTOMER_ID = s."customer_id"
);


-- 2. Load Product Dimension
INSERT INTO DIM_PRODUCTS (
    PRODUCT_ID,
    PRODUCT_CATEGORY_NAME,
    PRODUCT_CATEGORY_NAME_ENGLISH,
    PRODUCT_NAME_LENGTH,
    PRODUCT_DESCRIPTION_LENGTH,
    PRODUCT_PHOTOS_QTY,
    PRODUCT_WEIGHT_G,
    PRODUCT_LENGTH_CM,
    PRODUCT_HEIGHT_CM,
    PRODUCT_WIDTH_CM
)
SELECT
    p."product_id",
    p."product_category_name",
    t."product_category_name_english",
    p."product_name_lenght",
    p."product_description_lenght",
    p."product_photos_qty",
    p."product_weight_g",
    p."product_length_cm",
    p."product_height_cm",
    p."product_width_cm"
FROM SILVER.OLIST_PRODUCTS p
LEFT JOIN SILVER.PRODUCT_CATEGORY_NAME_TRANSLATION t
    ON p."product_category_name" = t."product_category_name"
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_PRODUCTS d
    WHERE d.PRODUCT_ID = p."product_id"
);


-- 3. Load Seller Dimension
INSERT INTO DIM_SELLERS (
    SELLER_ID,
    SELLER_ZIP_CODE_PREFIX,
    SELLER_CITY,
    SELLER_STATE
)
SELECT
    s."seller_id",
    s."seller_zip_code_prefix",
    s."seller_city",
    s."seller_state"
FROM SILVER.OLIST_SELLERS s
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_SELLERS d
    WHERE d.SELLER_ID = s."seller_id"
);


-- 4. Generate and Load Date Dimension
INSERT INTO DIM_DATE (
    DATE_KEY,
    FULL_DATE,
    DAY_OF_MONTH,
    DAY_NAME,
    DAY_OF_WEEK,
    WEEK_OF_YEAR,
    MONTH_NUMBER,
    MONTH_NAME,
    QUARTER_NUMBER,
    YEAR_NUMBER,
    IS_WEEKEND
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')),
    d,
    DAY(d),
    RTRIM(TO_CHAR(d, 'DAY')),
    DAYOFWEEKISO(d),
    WEEKISO(d),
    MONTH(d),
    RTRIM(TO_CHAR(d, 'MONTH')),
    QUARTER(d),
    YEAR(d),
    DAYOFWEEKISO(d) IN (6, 7)
FROM (
    SELECT DATEADD(
        DAY,
        SEQ4(),
        '2016-09-04'::DATE
    ) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 774))
)
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_DATE existing
    WHERE existing.FULL_DATE = d
);
