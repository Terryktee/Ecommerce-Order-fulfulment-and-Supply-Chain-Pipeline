import boto3
import psycopg2
import logging
import os
from airflow.decorators import task

# ==============================
# CONFIG
# ==============================

REGION = os.getenv("REGION")
WORKGROUP = os.getenv("WORKGROUP")
DB_NAME = os.getenv("DB_NAME")
ENDPOINT = os.getenv("ENDPOINT")

SQL_FILE_PATH = "/opt/airflow/data/gold/"
sql_file_names = [
    "create_schema","dim_customers","dim_date","dim_delivery",
    "dim_orders","dim_product","dim_shipping","fact_sales",
]


SILVER_SCHEMA = os.getenv("SILVER_SCHEMA")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def execute_sql(filename):

    conn = None
    cursor = None

    try:

        conn = get_redshift_connection()
        conn.set_isolation_level(0) 
        cursor = conn.cursor()
        # Read the SQL file content
        with open(filename, 'r') as f:
            sql_script = f.read()
        
        statements = [s.strip() for s in sql_script.split(";") if s.strip()]

        for statement in statements:
            cursor.execute(statement)
    except Exception as e:
        print(f"Error executing SQL file {filename}: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def build_path(file_name):
    return os.path.join(SQL_FILE_PATH, f"{file_name}.sql")

@task
def create_schema():
    execute_sql(build_path("create_schema"))


@task
def create_dim_customers():
    execute_sql(build_path("dim_customers"))


@task
def create_dim_date():
    execute_sql(build_path("dim_date"))


@task
def create_dim_delivery():
    execute_sql(build_path("dim_delivery"))


@task
def create_dim_orders():
    execute_sql(build_path("dim_orders"))


@task
def create_dim_product():
    execute_sql(build_path("dim_product"))


@task
def create_dim_shipping():
    execute_sql(build_path("dim_shipping"))


@task
def create_fact_sales():
    execute_sql(build_path("fact_sales"))