import boto3
import psycopg2
import logging
import sys
import os
from .transform_sql import run_gold_transformations
from .validation_sql import validate_gold_layer
# ==============================
# CONFIG
# ==============================

REGION = os.getenv("REGION")
WORKGROUP = os.getenv("WORKGROUP")
DB_NAME = os.getenv("DB_NAME")
ENDPOINT = os.getenv("ENDPOINT")

SILVER_SCHEMA = os.getenv("SILVER_SCHEMA")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    conn = None

    try:
        logger.info("🔑 Fetching temporary credentials...")

        client = boto3.client("redshift-serverless", region_name=REGION)

        response = client.get_credentials(
            workgroupName=WORKGROUP,
            dbName=DB_NAME,
            durationSeconds=3600,
        )

        logger.info("🔌 Connecting to Redshift...")

        conn = psycopg2.connect(
            host=ENDPOINT,
            port=5439,
            dbname=DB_NAME,
            user=response["dbUser"],
            password=response["dbPassword"],
            sslmode="require"
        )

        logger.info("✅ Connected to Redshift Serverless")

        # Run transformations
        run_gold_transformations(conn)

        # Validate
        validate_gold_layer(conn)

    except Exception as e:
        logger.error(f"Critical failure: {e}")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            logger.info("🔒 Connection closed.")


if __name__ == "__main__":
    main()
