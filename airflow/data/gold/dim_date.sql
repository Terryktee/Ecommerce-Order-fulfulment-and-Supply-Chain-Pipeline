DROP TABLE IF EXISTS gold.dim_date;

CREATE TABLE gold.dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE,
    day             INT,
    month           INT,
    month_name      VARCHAR(15),
    quarter         INT,
    year            INT,
    week_of_year    INT,
    day_of_week     INT,
    is_weekend      BOOLEAN
)
DISTSTYLE ALL
SORTKEY(date_key);

INSERT INTO gold.dim_date
SELECT DISTINCT
    TO_CHAR(d::timestamp,'YYYYMMDD')::INT,
    d::DATE,
    EXTRACT(DAY FROM d),
    EXTRACT(MONTH FROM d),
    TO_CHAR(d,'Month'),
    EXTRACT(QUARTER FROM d),
    EXTRACT(YEAR FROM d),
    EXTRACT(WEEK FROM d),
    EXTRACT(DOW FROM d),
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END
FROM (
    SELECT order_date::timestamp AS d
    FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfullment
    UNION
    SELECT shipping_date::timestamp
    FROM awsdatacatalog.lakehouse_silver.silver_supply_chain_order_fulfullment
) t;
