from airflow.decorators import task
import logging 
import psycopg2
import os 
import boto3

REGION = os.getenv("REGION")
WORKGROUP = os.getenv("WORKGROUP")
DB_NAME = os.getenv("DB_NAME")
ENDPOINT = os.getenv("ENDPOINT")

VALIDATE_GOLD = """
SELECT 
    COUNT(*) as total_records,
    SUM(sales_amount) as total_sales,
    SUM(profit_amount) as total_profit
FROM gold.fact_sales;
"""

def get_redshift_connection():
    client = boto3.client("redshift-serverless", region_name=REGION)

    response = client.get_credentials(
        workgroupName=WORKGROUP,
        dbName=DB_NAME,
        durationSeconds=3600,
    )

    conn = psycopg2.connect(
        host=ENDPOINT,
        port=5439,
        dbname=DB_NAME,
        user=response["dbUser"],
        password=response["dbPassword"],
        sslmode="require"
    )

    return conn

@task
def validate_gold():
    conn = get_redshift_connection()
    cursor = conn.cursor()

    cursor.execute(VALIDATE_GOLD)
    result = cursor.fetchone()

    logging.info(f"""
    ==============================
    GOLD VALIDATION RESULTS
    ==============================
    Total Records : {result[0]}
    Total Sales   : {result[1]}
    Total Profit  : {result[2]}
    ==============================
    """)

    cursor.close()
    conn.close()