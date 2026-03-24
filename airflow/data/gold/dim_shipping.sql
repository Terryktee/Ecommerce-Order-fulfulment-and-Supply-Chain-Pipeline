DROP TABLE IF EXISTS gold.dim_shipping;

CREATE TABLE gold.dim_shipping (
    shipping_key            INT IDENTITY(1,1),
    shipping_mode           VARCHAR(100) ENCODE ZSTD,
    shipping_days           INT ENCODE AZ64,
    shipment_scheduled_days INT ENCODE AZ64
)
DISTSTYLE ALL;

INSERT INTO gold.dim_shipping (shipping_mode)
SELECT DISTINCT shipping_mode
FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfullment;
