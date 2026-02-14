from airflow.decorators import task
import boto3
from botocore.exceptions import ClientError
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BUCKET = os.getenv("BUCKET")
PREFIX = "bronze/"
REGION = os.getenv("REGION")


@task
def get_latest_dataset() -> str:
    """
    Returns the S3 key of the most recently modified CSV
    under the bronze/ prefix.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=REGION,
        )
        s3 = session.client("s3")

        paginator = s3.get_paginator("list_objects_v2")
        files = []

        for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
            contents = page.get("Contents", [])
            files.extend(
                obj for obj in contents
                if obj["Key"].endswith(".csv")
            )

        if not files:
            raise ValueError("No CSV files found under bronze/")

        latest_file = max(files, key=lambda x: x["LastModified"])

        logger.info("Latest bronze dataset found: %s", latest_file["Key"])
        return latest_file["Key"]

    except ClientError:
        logger.exception("AWS error while listing bronze datasets")
        raise
    except Exception:
        logger.exception("Unexpected error while finding latest dataset")
        raise
