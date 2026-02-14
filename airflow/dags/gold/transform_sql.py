# utils/transform_sql.py

SILVER_SCHEMA = "awsdatacatalog.lakehouse_silver"

CREATE_GOLD_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS gold;
"""

CREATE_DIM_PRODUCTS = f"""
DROP TABLE IF EXISTS gold.dim_products;

CREATE TABLE gold.dim_products
DISTSTYLE ALL
AS
SELECT DISTINCT
    product_card_id::INT as product_id,
    product_name,
    category_name,
    department_name,
    NULLIF(product_price, 'NNNN')::DECIMAL(10,2) as price
FROM {SILVER_SCHEMA}.silver_supply_chain_order_fulfulment;
"""

CREATE_DIM_CUSTOMERS = f"""
DROP TABLE IF EXISTS gold.dim_customers;

CREATE TABLE gold.dim_customers
DISTSTYLE ALL
AS
SELECT DISTINCT
    customer_id::INT,
    customer_first_name as first_name,
    customer_last_name as last_name,
    customer_city as city,
    customer_state as state,
    customer_segment as segment
FROM {SILVER_SCHEMA}.silver_supply_chain_order_fulfulment;
"""

CREATE_FACT_SALES = f"""
DROP TABLE IF EXISTS gold.fact_sales;

CREATE TABLE gold.fact_sales
DISTKEY(order_id)
SORTKEY(order_date)
AS
SELECT 
    sales_id::INT,
    order_id::INT,
    customer_id::INT,
    product_card_id::INT as product_id,
    TO_TIMESTAMP(order_date, 'YYYY-MM-DD HH24:MI:SS') as order_date,
    TO_TIMESTAMP(shipping_date, 'YYYY-MM-DD HH24:MI:SS') as shipping_date,
    NULLIF(sales_amount, 'NNNN')::DECIMAL(18,2) as sales_amount,
    NULLIF(profit_per_order, 'NNNN')::DECIMAL(18,2) as profit,
    order_item_quantity::INT as quantity,
    delivery_status,
    shipping_mode
FROM {SILVER_SCHEMA}.silver_supply_chain_order_fulfulment;
"""
