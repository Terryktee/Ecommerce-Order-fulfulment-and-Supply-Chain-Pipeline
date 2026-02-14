from airflow.decorators import task
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

BUCKET = os.getenv("BUCKET")
REGION = os.getenv("REGION")

@task
def data_cleaning(bronze_key: str) -> str:
    """
    Cleans a bronze-layer CSV and writes a silver-layer dataset to S3.
    Ensures columns needed for 3NF are present and populated.
    """
    cleaned_date = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%H%M%S")

    silver_key = (
        f"silver/cleaned_date={cleaned_date}/"
        f"DataCoSupplyChainDataset_{timestamp}.csv"
    )

    local_path = Path("/tmp") / Path(bronze_key).name

    try:
        # -------------------------
        # AWS Session
        # -------------------------
        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=REGION,
        )
        s3 = session.client("s3")

        # -------------------------
        # Download Bronze
        # -------------------------
        logger.info("Downloading bronze file: %s", bronze_key)
        s3.download_file(BUCKET, bronze_key, str(local_path))
        logger.info("Downloaded to %s", local_path)

        # -------------------------
        # Read CSV
        # -------------------------
        df = pd.read_csv(local_path, encoding="latin1", low_memory=False)
        logger.info("Rows before cleaning: %s", len(df))

        # -------------------------
        # Drop sensitive columns
        # -------------------------
        sensitive_cols = [
            "Customer Password",
            "Product Description",
            "Customer Email",
            "Product Status",
        ]
        df = df.drop(columns=sensitive_cols, errors="ignore")

        # -------------------------
        # Normalize column names
        # -------------------------
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("[()]", "", regex=True)
        )

        # -------------------------
        # Rename columns
        # -------------------------
        df = df.rename(
            columns={
                "customer_fname": "customer_first_name",
                "customer_lname": "customer_last_name",
                "order_item_cardprod_id": "order_item_card_product_id",
                "type": "sales_type",
                "days_for_shipping_real": "shipping_days",
                "days_for_shipment_scheduled": "shipment_scheduled_days",
                "market": "sales_region",
                "order_date_dateorders": "order_date",
                "shipping_date_dateorders": "shipping_date",
                "benefit_per_order": "profit_per_order",
                "sales": "sales_amount",
            }
        )

        # -------------------------
        # Deduplication
        # -------------------------
        if {"order_id", "order_item_id"}.issubset(df.columns):
            df = df.drop_duplicates(subset=["order_id", "order_item_id"])

        # -------------------------
        # Date parsing
        # -------------------------
        for col in ["order_date", "shipping_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # -------------------------
        # Metrics
        # -------------------------
        if {"shipping_days", "shipment_scheduled_days"}.issubset(df.columns):
            df["is_late"] = (
                df["shipping_days"] > df["shipment_scheduled_days"]
            ).astype("int8")

        if {"profit_per_order", "sales_amount"}.issubset(df.columns):
            df["profit_margin"] = (
                df["profit_per_order"] / df["sales_amount"].replace(0, pd.NA)
            )

        # -------------------------
        # Normalize strings
        # -------------------------
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip().str.lower()

        if "delivery_status" in df.columns:
            df["delivery_status"] = df["delivery_status"].replace(
                {
                    "advanced shipping": "advanced",
                    "shipping on time": "on_time",
                    "late delivery": "late",
                    "shipping canceled": "canceled",
                }
            )
            
        # -------------------------
        # Write cleaned dataset
        # -------------------------
        df.to_csv(local_path, index=False)
        logger.info("Cleaned dataset written locally")

        # -------------------------
        # Upload Silver
        # -------------------------
        logger.info("Uploading silver dataset: %s", silver_key)
        s3.upload_file(str(local_path), BUCKET, silver_key)

        silver_path = f"s3://{BUCKET}/{silver_key}"
        logger.info("Silver dataset written to %s", silver_path)

        return silver_path

    except ClientError as e:
        logger.exception("AWS ClientError during data cleaning")
        raise e
    except Exception as e:
        logger.exception("Unexpected error during data cleaning")
        raise e
    finally:
        # -------------------------
        # Cleanup
        # -------------------------
        if local_path.exists():
            local_path.unlink()
