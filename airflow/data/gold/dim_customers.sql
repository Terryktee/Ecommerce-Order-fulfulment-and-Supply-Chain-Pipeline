DROP TABLE IF EXISTS gold.dim_customer;

CREATE TABLE gold.dim_customer (
    customer_key       INT IDENTITY(1,1),
    customer_id        INT NOT NULL,
    first_name         VARCHAR(100) ENCODE ZSTD,
    last_name          VARCHAR(100) ENCODE ZSTD,
    segment            VARCHAR(100) ENCODE ZSTD,
    city               VARCHAR(100) ENCODE ZSTD,
    state              VARCHAR(100) ENCODE ZSTD,
    country            VARCHAR(100) ENCODE ZSTD,
    zipcode            VARCHAR(20) ENCODE ZSTD,
    street             VARCHAR(255) ENCODE ZSTD,
    latitude           DECIMAL(9,6) ENCODE AZ64,
    longitude          DECIMAL(9,6) ENCODE AZ64,
    record_start_date  DATE DEFAULT CURRENT_DATE,
    record_end_date    DATE DEFAULT '9999-12-31',
    is_current         BOOLEAN DEFAULT TRUE
)
DISTSTYLE AUTO;

INSERT INTO gold.dim_customer (
    customer_id,
    first_name,
    last_name,
    segment,
    city,
    state,
    country,
    zipcode,
    street,
    latitude,
    longitude
)
SELECT DISTINCT
    customer_id::INT,
    customer_first_name,
    customer_last_name,
    customer_segment,
    customer_city,
    customer_state,
    customer_country,
    customer_zipcode,
    customer_street,
    latitude::DECIMAL(9,6),
    longitude::DECIMAL(9,6)
FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfulment;

