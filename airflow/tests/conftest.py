import os
import pytest
from airflow.models import DagBag
import boto3
import psycopg2

@pytest.fixture
def dagbag():
    yield DagBag()

@pytest.fixture
def s3_client():
    return boto3.client("s3")


@pytest.fixture          
def real_redshift_connection():

    DB_NAME= os.getenv("DB_NAME")
    WORKGROUP= os.getenv("WORKGROUP")
    ENDPOINT = os.getenv("ENDPOINT")
    REGION = os.getenv("REGION")
   

    conn = None

    client = boto3.client("redshift-serverless", region_name=REGION)

    response = client.get_credentials(
        workgroupName=WORKGROUP,
        dbName=DB_NAME,
        durationSeconds=3600,
    )

    try:

        conn = psycopg2.connect(
            host=ENDPOINT,
            port=5439,
            dbname=DB_NAME,
            user=response["dbUser"],
            password=response["dbPassword"],
            sslmode="require"
        )

        yield conn

    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to database:{e}")
    finally:
        if conn:
            conn.close()
