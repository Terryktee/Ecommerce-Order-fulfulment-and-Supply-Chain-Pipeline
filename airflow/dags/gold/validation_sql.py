# utils/validation_sql.py

VALIDATE_GOLD = """
SELECT 
    COUNT(*) as total_records,
    SUM(sales_amount) as total_sales,
    SUM(profit) as total_profit
FROM gold.fact_sales;
"""
