CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    customer_key BIGINT NOT NULL REFERENCES gold.dim_customers(customer_key),
    purchase_date_key INTEGER REFERENCES gold.dim_date(date_key),
    order_status VARCHAR(30),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.fact_order_items (
    order_item_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES gold.dim_customers(customer_key),
    product_key BIGINT NOT NULL REFERENCES gold.dim_products(product_key),
    seller_key BIGINT NOT NULL REFERENCES gold.dim_sellers(seller_key),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(12,2),
    freight_value NUMERIC(12,2),
    UNIQUE (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_payments (
    payment_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES gold.dim_customers(customer_key),
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(30),
    payment_installments INTEGER,
    payment_value NUMERIC(12,2),
    UNIQUE (order_id, payment_sequential)
);
