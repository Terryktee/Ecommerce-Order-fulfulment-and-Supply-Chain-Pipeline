import pandas as pd
from airflow.decorators import task

@task
def load_silver_csv(s3_path: str) -> pd.DataFrame:
    import boto3, os, logging, io
    import pandas as pd

    BUCKET = os.getenv("BUCKET")
    REGION = os.getenv("REGION")

    if s3_path.startswith("s3://"):
        s3_path = s3_path.replace(f"s3://{BUCKET}/", "")

    logging.info(f"S3 PATH USED: {s3_path}")

    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )

    s3 = session.client("s3")
    body = s3.get_object(Bucket=BUCKET, Key=s3_path)["Body"].read()

    logging.info(f"S3 OBJECT SIZE: {len(body)} bytes")
    logging.info(f"FIRST 200 BYTES: {body[:200]}")

    # 🔑 Decode bytes → text
    text = body.decode("latin1", errors="replace")

    df = pd.read_csv(
        io.StringIO(text),
        sep=",",
        engine="python",
        dtype=str
    )

    df.columns = df.columns.str.strip()

    logging.info(f"Loaded dataframe: {df.shape[0]} rows × {df.shape[1]} columns")

    return df




