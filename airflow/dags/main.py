import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import timedelta,datetime
from bronze.data_upload import upload_file_to_s3
from silver.modules import data_cleaning,get_latest_dataset,pipeline
from silver.modules import load_data
from gold.load_to_redshift import (
    create_dim_customers,create_dim_date,create_dim_delivery,create_dim_orders,
    create_dim_product,create_dim_shipping,create_fact_sales,create_schema
)
from gold.validation_sql import validate_gold
from data_quality.soda import supply_chain_data_quality


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

soda_tables_to_test = [
   "dim_customer",
    "dim_date",
    "dim_delivery",
    "dim_order",
    "dim_product",
    "dim_shipping",
    "fact_sales",
]


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
    #"retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Bronze_Supply_Chain_Ingest",
    default_args=default_args,
    description="DAG to read the local file and rename it",
    schedule="0 14 * * *",
    catchup=False,

) as dag:
    
    upload_to_aws = upload_file_to_s3(
        file_name="/opt/airflow/data/bronze/DataCoSupplyChainDataset.csv",
        bucket="amzon-s3-ecommerce-order-fulfillment",
        object_name=object_name,
        )
    
    trigger_silver = TriggerDagRunOperator(
        task_id  = "trigger_silver",
        trigger_dag_id = "Silver_Supply_Chain_Transform"
    )

    upload_to_aws >> trigger_silver

with DAG(
    dag_id="Silver_Supply_Chain_Transform",
    default_args=default_args,
    description="DAG to extract data from aws , then clean it then transform it then export to delta",
    schedule=None,
    catchup=False,
) as dag:
    
    base_path = "s3://amzon-s3-ecommerce-order-fulfillment/silver/supply_chain_order_fulfullment"

    get_dataset = get_latest_dataset.get_latest_dataset()
    cleaned_data_path = data_cleaning.data_cleaning(get_dataset)
    
    converting_to_delta_table = pipeline.run_pipeline(cleaned_data_path,base_path)
    
    trigger_gold = TriggerDagRunOperator(
        task_id = "trigger_gold",
        trigger_dag_id = "Gold_Supply_Chain_Star_Schema"
    )
    get_dataset >> cleaned_data_path  >> converting_to_delta_table >> trigger_gold

with DAG(
    dag_id="Gold_Supply_Chain_Star_Schema",
    default_args=default_args,
    description="DAG to build Star Schema",
    schedule=None,
    catchup=False,
) as dag:

    schema = create_schema()

    dim_customers = create_dim_customers()
    dim_date = create_dim_date()
    dim_delivery = create_dim_delivery()
    dim_orders = create_dim_orders()
    dim_product = create_dim_product()
    dim_shipping = create_dim_shipping()
    fact_sales = create_fact_sales()
    sql_validation = validate_gold()

    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_data_quality",
        trigger_dag_id = "Gold_Supply_Chain_Data_Quality_Check"
    )
    # Dependencies
    schema >> [
        dim_customers,
        dim_date,
        dim_delivery,
        dim_orders,
        dim_product,
        dim_shipping,
    ]

    [
        dim_customers,
        dim_date,
        dim_delivery,
        dim_orders,
        dim_product,
        dim_shipping,
    ] >> fact_sales >> sql_validation >> trigger_data_quality



with DAG(
   dag_id="Gold_Supply_Chain_Data_Quality_Check",
   default_args=default_args,
   description="DAG to check the data quality of the data uploaded to data warehouse",
   schedule=None,
   catchup=False,
) as dag:

    soda_tasks = []

    for table in soda_tables_to_test:
        task = supply_chain_data_quality(table)
        soda_tasks.append(task)

