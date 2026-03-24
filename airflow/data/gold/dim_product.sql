DROP TABLE IF EXISTS gold.dim_product;

CREATE TABLE gold.dim_product (
    product_key        INT IDENTITY(1,1),
    product_id         INT NOT NULL,
    product_name       VARCHAR(255) ENCODE ZSTD,
    category_id        VARCHAR(50) ENCODE ZSTD,
    category_name      VARCHAR(150) ENCODE ZSTD,
    department_id      VARCHAR(50) ENCODE ZSTD,
    department_name    VARCHAR(150) ENCODE ZSTD,
    product_image      VARCHAR(500) ENCODE ZSTD,
    price              DECIMAL(10,2) ENCODE AZ64,
    record_start_date  DATE DEFAULT CURRENT_DATE,
    record_end_date    DATE DEFAULT '9999-12-31',
    is_current         BOOLEAN DEFAULT TRUE
)
DISTSTYLE ALL;

INSERT INTO gold.dim_product (
    product_id,
    product_name,
    category_id,
    category_name,
    department_id,
    department_name,
    product_image,
    price
)
SELECT DISTINCT
    CAST(product_card_id AS INT),

    NULLIF(TRIM(product_name), ''),
    NULLIF(TRIM(category_id), ''),
    NULLIF(TRIM(category_name), ''),
    NULLIF(TRIM(department_id), ''),
    NULLIF(TRIM(department_name), ''),
    NULLIF(TRIM(product_image), ''),

    CASE 
        WHEN product_price IS NULL THEN NULL
        WHEN TRIM(product_price) IN ('', 'NNNN', 'NULL') THEN NULL
        WHEN REGEXP_INSTR(product_price, '^[0-9]+(\.[0-9]+)?$') = 1
            THEN CAST(product_price AS DECIMAL(10,2))
        ELSE NULL
    END AS price

FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfullment;