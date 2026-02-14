from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import timedelta,datetime
from bronze.data_upload import upload_file
from silver.modules import data_cleaning,get_latest_dataset,pipeline
from silver.modules import load_data
from airflow.decorators import task
import boto3
import psycopg2
from gold.transform_sql import (
    CREATE_GOLD_SCHEMA,
    CREATE_DIM_PRODUCTS,
    CREATE_DIM_CUSTOMERS,
    CREATE_FACT_SALES,
)
from gold.validation_sql import VALIDATE_GOLD
import logging
import os

local_tz = pendulum.timezone("Africa/Harare")
ingestion_date = datetime.now().strftime("%Y-%m-%d")

REGION = os.getenv("REGION")
WORKGROUP = os.getenv("WORKGROUP")
DB_NAME = os.getenv("DB_NAME")
ENDPOINT = os.getenv("ENDPOINT")

object_name = (
    f"bronze/"
    f"ingestion_date={ingestion_date}/"
    f"DataCoSupplyChainDataset_{datetime.now().strftime('%H%M%S')}.csv"
)



default_args={
    "owner":"dataEngineer",
    "depends_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"ttkapumhaa@gmail.com",
    "max_active_runs":1,
    "dagrun_timeout":timedelta(hours=1),
    "start_date":datetime(2026, 2, 3, tzinfo=local_tz),
   # "retries":1,
   # "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Ingestion",
    default_args=default_args,
    description="DAT to read the local file and rename it",
    schedule="0 14 * * *",
    catchup=False,

) as dag_file_rename:
    
    upload_to_aws = upload_file(
        file_name="/opt/airflow/data/bronze/DataCoSupplyChainDataset.csv",
        bucket="amzon-s3-ecommerce-order-fulfillment",
        object_name=object_name,
        )
    
    upload_to_aws

base_path = "s3://amzon-s3-ecommerce-order-fulfillment/silver"


with DAG(
    dag_id="Transformation",
    default_args=default_args,
    description="DAG to extract data from aws , then clean it then transform it then export to delta",
    schedule="0 15 * * * ",
    catchup=False,
) as dag_file_name:

    bronze_task = get_latest_dataset.get_latest_dataset()
    silver_task = data_cleaning.data_cleaning(bronze_task)
    df_task = load_data.load_silver_csv(silver_task)
    pipeline_task = pipeline.run_pipeline(df_task, base_path)
    
    bronze_task >> silver_task >> df_task >> pipeline_task

with DAG(
    dag_id="silver_to_gold_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["redshift", "gold-layer"],
) as dag:

    # ==============================
    # Connection Helper
    # ==============================

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


    # ==============================
    # TASK 1: Transform
    # ==============================

    @task
    def transform_gold():
        conn = get_redshift_connection()
        cursor = conn.cursor()

        cursor.execute(CREATE_GOLD_SCHEMA)
        cursor.execute(CREATE_DIM_PRODUCTS)
        cursor.execute(CREATE_DIM_CUSTOMERS)
        cursor.execute(CREATE_FACT_SALES)

        conn.commit()
        cursor.close()
        conn.close()


    # ==============================
    # TASK 2: Validate
    # ==============================

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


    # ==============================
    # TASK ORDER
    # ==============================

    transform_gold() >> validate_gold()