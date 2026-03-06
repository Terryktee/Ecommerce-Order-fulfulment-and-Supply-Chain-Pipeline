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
    product_card_id::INT,
    product_name,
    category_id,
    category_name,
    department_id,
    department_name,
    product_image,
    NULLIF(product_price,'NNNN')::DECIMAL(10,2)
FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfulment;

