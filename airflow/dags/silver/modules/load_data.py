import pandas as pd
from airflow.decorators import task
import boto3, os, logging, io

@task
def load_silver_csv(df: pd.DataFrame) -> str:
    """
    Receives a DataFrame, saves it to S3, and returns the S3 Path string.
    """
    BUCKET = os.getenv("BUCKET")
    REGION = os.getenv("REGION")
    
    # Define where the cleaned file should go
    # Using a fixed name or dynamic timestamp is fine
    output_key = "silver/temp_cleaned_data.csv"
    output_path = f"s3://{BUCKET}/{output_key}"

    logging.info(f"Saving cleaned DataFrame to: {output_path}")

    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )
    s3 = session.client("s3")

    # Convert DF to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3.put_object(
        Bucket=BUCKET, 
        Key=output_key, 
        Body=csv_buffer.getvalue()
    )

    logging.info(f"Successfully saved {len(df)} rows to S3.")

    # RETURN THE STRING PATH
    return output_path