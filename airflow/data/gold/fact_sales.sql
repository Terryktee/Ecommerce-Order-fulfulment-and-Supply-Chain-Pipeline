DROP TABLE IF EXISTS gold.fact_sales;

CREATE TABLE gold.fact_sales (
    sales_key          BIGINT IDENTITY(1,1),

    order_key          INT,
    customer_key       INT,
    product_key        INT,
    shipping_key       INT,
    delivery_key       INT,

    order_date_key     INT,
    shipping_date_key  INT,

    quantity           INT ENCODE AZ64,
    sales_amount       DECIMAL(18,2) ENCODE AZ64,
    discount_amount    DECIMAL(10,2) ENCODE AZ64,
    profit_amount      DECIMAL(18,2) ENCODE AZ64,
    profit_ratio       DECIMAL(5,4) ENCODE AZ64
)
DISTSTYLE KEY
DISTKEY(customer_key)
INTERLEAVED SORTKEY (
    order_date_key,
    product_key
);

INSERT INTO gold.fact_sales (
    order_key,
    customer_key,
    product_key,
    shipping_key,
    delivery_key,
    order_date_key,
    shipping_date_key,
    quantity,
    sales_amount,
    discount_amount,
    profit_amount,
    profit_ratio
)
SELECT
    o.order_key,
    c.customer_key,
    p.product_key,
    s.shipping_key,
    d.delivery_key,
    TO_CHAR(f.order_date::timestamp,'YYYYMMDD')::INT,
    TO_CHAR(f.shipping_date::timestamp,'YYYYMMDD')::INT,
    f.order_item_quantity::INT,
    NULLIF(f.sales_amount,'NNNN')::DECIMAL(18,2),
    NULLIF(f.order_item_discount,'NNNN')::DECIMAL(10,2),
    NULLIF(f.profit_per_order,'NNNN')::DECIMAL(18,2),
    NULLIF(f.order_item_profit_ratio,'NNNN')::DECIMAL(5,4)
FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfulment f
JOIN gold.dim_customer c ON f.customer_id = c.customer_id
JOIN gold.dim_product p ON f.product_card_id = p.product_id
JOIN gold.dim_order o ON f.order_id = o.order_id
JOIN gold.dim_shipping s ON f.shipping_mode = s.shipping_mode
JOIN gold.dim_delivery d ON f.delivery_status = d.delivery_status;