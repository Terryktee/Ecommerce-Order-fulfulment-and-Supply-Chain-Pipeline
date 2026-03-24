# silver/modules/write_delta.py

import os
import logging
import boto3
import awswrangler as wr
import pyarrow as pa
from deltalake.writer import write_deltalake


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def convert_s3_to_delta(
    source_s3_path: str,
    target_delta_path: str,
    partition_by=None,
):
    """
    Reads CSV from S3 and writes a Delta Lake table back to S3.
    Fully compatible with Airflow + Delta Lake (Rust engine).
    """
    
    if not isinstance(source_s3_path, str):
        raise ValueError(f"source_s3_path must be string, got {type(source_s3_path)}")

    if not source_s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {source_s3_path}")

    # Clean target path
    target_delta_path = target_delta_path.rstrip("/")

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION = os.getenv("REGION")

    if not all([AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, REGION]):
        raise ValueError("Missing AWS environment variables")

    logger.info(f"Reading CSV from: {source_s3_path}")
    logger.info(f"Writing Delta table to: {target_delta_path}")

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION,
    )

    try:
        df = wr.s3.read_csv(
            path=source_s3_path,
            boto3_session=session,
            encoding="latin1",
        )
    except Exception as e:
        logger.exception("Failed to read CSV from S3")
        raise

    if df is None or df.empty:
        raise ValueError("DataFrame is empty — nothing to write")

    logger.info(f"DataFrame loaded with shape: {df.shape}")

  
    try:
        df = df.convert_dtypes()
    except Exception:
        logger.warning("convert_dtypes failed, forcing string conversion")
        df = df.astype(str)


    try:
        table = pa.Table.from_pandas(df)
    except Exception:
        logger.exception("Arrow conversion failed — forcing all columns to string")
        df = df.astype(str)
        table = pa.Table.from_pandas(df)


    try:
        write_deltalake(
            table_or_uri=target_delta_path,
            data=table,
            mode="overwrite",
            partition_by=partition_by,
            storage_options={
                "aws_access_key_id": AWS_ACCESS_KEY,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                "region": REGION,
                "aws_endpoint_url": f"https://s3.{REGION}.amazonaws.com",
            },
        )
    except Exception:
        logger.exception("Delta Lake write failed")
        raise

    logger.info("Delta table write successful")

    return target_delta_path