CREATE OR REPLACE VIEW gold.v_monthly_sales AS
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS product_sales,
    SUM(oi.freight_value) AS freight_revenue,
    SUM(oi.price + oi.freight_value) AS total_sales,
    ROUND(
        SUM(oi.price) / NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    ) AS average_order_value
FROM gold.fact_order_items oi
JOIN gold.fact_orders o ON oi.order_id = o.order_id
JOIN gold.dim_date d ON o.purchase_date_key = d.date_key
GROUP BY d.year_number, d.month_number, d.month_name;


CREATE OR REPLACE VIEW gold.v_customer_sales AS
SELECT
    dc.customer_unique_id,
    dc.customer_city,
    dc.customer_state,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(foi.price) AS total_product_spend,
    SUM(foi.freight_value) AS total_freight,
    ROUND(
        SUM(foi.price) / NULLIF(COUNT(DISTINCT fo.order_id), 0),
        2
    ) AS average_order_value
FROM gold.dim_customers dc
JOIN gold.fact_orders fo ON dc.customer_key = fo.customer_key
JOIN gold.fact_order_items foi ON fo.order_id = foi.order_id
GROUP BY dc.customer_unique_id, dc.customer_city, dc.customer_state;


CREATE OR REPLACE VIEW gold.v_product_performance AS
SELECT
    dp.product_id,
    dp.product_category_name_english AS product_category,
    COUNT(DISTINCT foi.order_id) AS total_orders,
    COUNT(*) AS units_sold,
    SUM(foi.price) AS product_sales,
    SUM(foi.freight_value) AS freight_revenue,
    ROUND(AVG(foi.price), 2) AS average_product_price
FROM gold.fact_order_items foi
JOIN gold.dim_products dp ON foi.product_key = dp.product_key
GROUP BY dp.product_id, dp.product_category_name_english;


CREATE OR REPLACE VIEW gold.v_seller_performance AS
SELECT
    ds.seller_id,
    ds.seller_city,
    ds.seller_state,
    COUNT(DISTINCT foi.order_id) AS total_orders,
    COUNT(*) AS units_sold,
    SUM(foi.price) AS product_sales,
    SUM(foi.freight_value) AS freight_revenue,
    ROUND(AVG(foi.price), 2) AS average_item_price
FROM gold.fact_order_items foi
JOIN gold.dim_sellers ds ON foi.seller_key = ds.seller_key
GROUP BY ds.seller_id, ds.seller_city, ds.seller_state;


CREATE OR REPLACE VIEW gold.v_payment_analysis AS
SELECT
    payment_type,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_value) AS total_payment_value,
    ROUND(AVG(payment_value), 2) AS average_payment_value,
    SUM(payment_installments) AS total_installments
FROM gold.fact_payments
GROUP BY payment_type;


CREATE OR REPLACE VIEW gold.v_delivery_performance AS
SELECT
    order_status,
    COUNT(*) AS total_orders,
    COUNT(order_delivered_customer_date) AS delivered_orders,
    COUNT(*) FILTER (
        WHERE order_delivered_customer_date > order_estimated_delivery_date
    ) AS late_deliveries,
    COUNT(*) FILTER (
        WHERE order_delivered_customer_date <= order_estimated_delivery_date
    ) AS on_time_deliveries,
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM (
                order_delivered_customer_date - order_purchase_timestamp
            )) / 86400
        )::NUMERIC,
        2
    ) AS avg_delivery_days
FROM gold.fact_orders
GROUP BY order_status;


CREATE OR REPLACE VIEW gold.v_review_analysis AS
SELECT
    r.review_score,
    COUNT(*) AS total_reviews,
    COUNT(DISTINCT r.order_id) AS reviewed_orders,
    COUNT(r.review_comment_message) AS reviews_with_comments,
    ROUND(AVG(LENGTH(r.review_comment_message)), 2) AS avg_comment_length
FROM public.olist_order_reviews r
JOIN gold.fact_orders o ON r.order_id = o.order_id
GROUP BY r.review_score;
