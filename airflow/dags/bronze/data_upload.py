import logging
import boto3
from botocore.exceptions import ClientError
from airflow.decorators import task
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

@task
def upload_file(file_name, bucket, object_name=None):
    if not os.path.isfile(file_name):
        raise FileNotFoundError(f"File not found: {file_name}")

    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_access_key:
        raise ValueError("AWS credentials not found")

    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        region_name="eu-north-1",
    )

    s3_client = session.client("s3")

    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        logger.info(f"Uploaded {file_name} to s3://{bucket}/{object_name}")
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        raise

    return f"s3://{bucket}/{object_name}"
