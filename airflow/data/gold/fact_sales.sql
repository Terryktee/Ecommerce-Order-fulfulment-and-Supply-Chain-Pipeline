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

    -- Safe date handling
    CASE 
        WHEN f.order_date IS NULL THEN NULL
        ELSE TO_CHAR(f.order_date::TIMESTAMP, 'YYYYMMDD')::INT
    END AS order_date_key,

    CASE 
        WHEN f.shipping_date IS NULL THEN NULL
        ELSE TO_CHAR(f.shipping_date::TIMESTAMP, 'YYYYMMDD')::INT
    END AS shipping_date_key,

    -- Safe numeric casting
    CAST(f.order_item_quantity AS INT),

    CASE 
        WHEN f.sales_amount IS NULL THEN NULL
        WHEN TRIM(f.sales_amount) IN ('', 'NNNN', 'NULL') THEN NULL
        WHEN REGEXP_INSTR(f.sales_amount, '^[0-9]+(\.[0-9]+)?$') = 1
            THEN CAST(f.sales_amount AS DECIMAL(18,2))
        ELSE NULL
    END AS sales_amount,

    CASE 
        WHEN f.order_item_discount IS NULL THEN NULL
        WHEN TRIM(f.order_item_discount) IN ('', 'NNNN', 'NULL') THEN NULL
        WHEN REGEXP_INSTR(f.order_item_discount, '^[0-9]+(\.[0-9]+)?$') = 1
            THEN CAST(f.order_item_discount AS DECIMAL(10,2))
        ELSE NULL
    END AS discount_amount,

    CASE 
        WHEN f.profit_per_order IS NULL THEN NULL
        WHEN TRIM(f.profit_per_order) IN ('', 'NNNN', 'NULL') THEN NULL
        WHEN REGEXP_INSTR(f.profit_per_order, '^[0-9]+(\.[0-9]+)?$') = 1
            THEN CAST(f.profit_per_order AS DECIMAL(18,2))
        ELSE NULL
    END AS profit_amount,

    CASE 
        WHEN f.order_item_profit_ratio IS NULL THEN NULL
        WHEN TRIM(f.order_item_profit_ratio) IN ('', 'NNNN', 'NULL') THEN NULL
        WHEN REGEXP_INSTR(f.order_item_profit_ratio, '^[0-9]+(\.[0-9]+)?$') = 1
            THEN CAST(f.order_item_profit_ratio AS DECIMAL(5,4))
        ELSE NULL
    END AS profit_ratio

FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfullment f

JOIN gold.dim_customer c 
    ON f.customer_id = c.customer_id
    AND c.is_current = TRUE

JOIN gold.dim_product p 
    ON f.product_card_id = p.product_id
    AND p.is_current = TRUE

JOIN gold.dim_order o 
    ON f.order_id = o.order_id

JOIN gold.dim_shipping s 
    ON f.shipping_mode = s.shipping_mode

JOIN gold.dim_delivery d 
    ON f.delivery_status = d.delivery_status;