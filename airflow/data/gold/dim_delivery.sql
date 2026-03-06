DROP TABLE IF EXISTS gold.dim_delivery;

CREATE TABLE gold.dim_delivery (
    delivery_key        INT IDENTITY(1,1),
    delivery_status     VARCHAR(100) ENCODE ZSTD,
    late_delivery_risk  VARCHAR(50) ENCODE ZSTD
)
DISTSTYLE ALL;

INSERT INTO gold.dim_delivery (delivery_status)
SELECT DISTINCT delivery_status
FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfulment;

