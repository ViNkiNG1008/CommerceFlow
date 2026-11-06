-- 1. Dimension row counts
SELECT 'dim_customers' AS table_name, COUNT(*) AS row_count FROM gold.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'dim_sellers', COUNT(*) FROM gold.dim_sellers
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date;


-- 2. Fact row counts
SELECT 'fact_orders' AS table_name, COUNT(*) AS row_count FROM gold.fact_orders
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM gold.fact_order_items
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM gold.fact_payments;


-- 3. Check missing dimension relationships
SELECT
    'fact_orders -> dim_customers' AS relationship,
    COUNT(*) AS invalid_records
FROM gold.fact_orders f
LEFT JOIN gold.dim_customers d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL

UNION ALL

SELECT
    'fact_order_items -> dim_products',
    COUNT(*)
FROM gold.fact_order_items f
LEFT JOIN gold.dim_products d ON f.product_key = d.product_key
WHERE d.product_key IS NULL

UNION ALL

SELECT
    'fact_order_items -> dim_sellers',
    COUNT(*)
FROM gold.fact_order_items f
LEFT JOIN gold.dim_sellers d ON f.seller_key = d.seller_key
WHERE d.seller_key IS NULL

UNION ALL

SELECT
    'fact_payments -> dim_customers',
    COUNT(*)
FROM gold.fact_payments f
LEFT JOIN gold.dim_customers d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL;


-- 4. Check date dimension relationship
SELECT
    COUNT(*) AS orders_without_purchase_date_key
FROM gold.fact_orders
WHERE purchase_date_key IS NULL;


-- 5. Test analytical views
SELECT * FROM gold.v_monthly_sales LIMIT 5;
SELECT * FROM gold.v_customer_sales LIMIT 5;
SELECT * FROM gold.v_product_performance LIMIT 5;
SELECT * FROM gold.v_seller_performance LIMIT 5;
SELECT * FROM gold.v_payment_analysis;
SELECT * FROM gold.v_delivery_performance;
SELECT * FROM gold.v_review_analysis;
