DROP TABLE IF EXISTS gold.dim_order;

CREATE TABLE gold.dim_order (
    order_key      INT IDENTITY(1,1),
    order_id       INT NOT NULL,
    order_status   VARCHAR(100) ENCODE ZSTD,
    sales_type     VARCHAR(100) ENCODE ZSTD,
    order_region   VARCHAR(100) ENCODE ZSTD
)
DISTSTYLE AUTO;

INSERT INTO gold.dim_order (
    order_id,
    order_status,
    sales_type,
    order_region
)
SELECT DISTINCT
    order_id::INT,
    order_status,
    sales_type,
    order_region
FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfullment;