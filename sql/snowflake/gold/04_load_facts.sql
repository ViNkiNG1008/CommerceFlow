
-- =====================================================
-- CommerceFlow: Snowflake Gold Fact Loading
-- =====================================================

USE DATABASE COMMERCEFLOW_DB;
USE SCHEMA GOLD;


-- 1. Load Orders Fact Table
INSERT INTO FACT_ORDERS (
    ORDER_ID,
    CUSTOMER_KEY,
    PURCHASE_DATE_KEY,
    ORDER_STATUS,
    ORDER_PURCHASE_TIMESTAMP,
    ORDER_APPROVED_AT,
    ORDER_DELIVERED_CARRIER_DATE,
    ORDER_DELIVERED_CUSTOMER_DATE,
    ORDER_ESTIMATED_DELIVERY_DATE
)
SELECT
    o."order_id",
    dc.CUSTOMER_KEY,
    d.DATE_KEY,
    o."order_status",
    TRY_TO_TIMESTAMP_NTZ(o."order_purchase_timestamp"::VARCHAR),
    TRY_TO_TIMESTAMP_NTZ(o."order_approved_at"::VARCHAR),
    TRY_TO_TIMESTAMP_NTZ(o."order_delivered_carrier_date"::VARCHAR),
    TRY_TO_TIMESTAMP_NTZ(o."order_delivered_customer_date"::VARCHAR),
    TRY_TO_TIMESTAMP_NTZ(o."order_estimated_delivery_date"::VARCHAR)
FROM SILVER.OLIST_ORDERS o
JOIN DIM_CUSTOMERS dc
    ON o."customer_id" = dc.CUSTOMER_ID
JOIN DIM_DATE d
    ON TO_DATE(
        TRY_TO_TIMESTAMP_NTZ(o."order_purchase_timestamp"::VARCHAR)
    ) = d.FULL_DATE
WHERE NOT EXISTS (
    SELECT 1
    FROM FACT_ORDERS f
    WHERE f.ORDER_ID = o."order_id"
);


-- 2. Load Order Items Fact Table
INSERT INTO FACT_ORDER_ITEMS (
    ORDER_ID,
    ORDER_ITEM_ID,
    CUSTOMER_KEY,
    PRODUCT_KEY,
    SELLER_KEY,
    SHIPPING_LIMIT_DATE,
    PRICE,
    FREIGHT_VALUE
)
SELECT
    oi."order_id",
    oi."order_item_id",
    dc.CUSTOMER_KEY,
    dp.PRODUCT_KEY,
    ds.SELLER_KEY,
    TRY_TO_TIMESTAMP_NTZ(oi."shipping_limit_date"::VARCHAR),
    oi."price",
    oi."freight_value"
FROM SILVER.OLIST_ORDER_ITEMS oi
JOIN SILVER.OLIST_ORDERS o
    ON oi."order_id" = o."order_id"
JOIN DIM_CUSTOMERS dc
    ON o."customer_id" = dc.CUSTOMER_ID
JOIN DIM_PRODUCTS dp
    ON oi."product_id" = dp.PRODUCT_ID
JOIN DIM_SELLERS ds
    ON oi."seller_id" = ds.SELLER_ID
WHERE NOT EXISTS (
    SELECT 1
    FROM FACT_ORDER_ITEMS f
    WHERE f.ORDER_ID = oi."order_id"
      AND f.ORDER_ITEM_ID = oi."order_item_id"
);


-- 3. Load Payments Fact Table
INSERT INTO FACT_PAYMENTS (
    ORDER_ID,
    CUSTOMER_KEY,
    PAYMENT_SEQUENTIAL,
    PAYMENT_TYPE,
    PAYMENT_INSTALLMENTS,
    PAYMENT_VALUE
)
SELECT
    p."order_id",
    dc.CUSTOMER_KEY,
    p."payment_sequential",
    p."payment_type",
    p."payment_installments",
    p."payment_value"
FROM SILVER.OLIST_ORDER_PAYMENTS p
JOIN SILVER.OLIST_ORDERS o
    ON p."order_id" = o."order_id"
JOIN DIM_CUSTOMERS dc
    ON o."customer_id" = dc.CUSTOMER_ID
WHERE NOT EXISTS (
    SELECT 1
    FROM FACT_PAYMENTS f
    WHERE f.ORDER_ID = p."order_id"
      AND f.PAYMENT_SEQUENTIAL = p."payment_sequential"
);
