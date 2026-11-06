INSERT INTO gold.fact_orders (
    order_id,
    customer_key,
    purchase_date_key,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
SELECT
    o.order_id,
    dc.customer_key,
    d.date_key,
    o.order_status,
    TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(o.order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS')
FROM public.olist_orders o
JOIN gold.dim_customers dc
    ON o.customer_id = dc.customer_id
JOIN gold.dim_date d
    ON TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS')::DATE = d.full_date
ON CONFLICT (order_id) DO NOTHING;


INSERT INTO gold.fact_order_items (
    order_id,
    order_item_id,
    customer_key,
    product_key,
    seller_key,
    shipping_limit_date,
    price,
    freight_value
)
SELECT
    oi.order_id,
    oi.order_item_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    TO_TIMESTAMP(oi.shipping_limit_date, 'YYYY-MM-DD HH24:MI:SS'),
    oi.price,
    oi.freight_value
FROM public.olist_order_items oi
JOIN public.olist_orders o
    ON oi.order_id = o.order_id
JOIN gold.dim_customers dc
    ON o.customer_id = dc.customer_id
JOIN gold.dim_products dp
    ON oi.product_id = dp.product_id
JOIN gold.dim_sellers ds
    ON oi.seller_id = ds.seller_id
ON CONFLICT (order_id, order_item_id) DO NOTHING;


INSERT INTO gold.fact_payments (
    order_id,
    customer_key,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    p.order_id,
    dc.customer_key,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value
FROM public.olist_order_payments p
JOIN public.olist_orders o
    ON p.order_id = o.order_id
JOIN gold.dim_customers dc
    ON o.customer_id = dc.customer_id
ON CONFLICT (order_id, payment_sequential) DO NOTHING;
