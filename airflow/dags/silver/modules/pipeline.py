# silver/modules/pipeline.py
from .write_delta import convert_s3_to_delta
from airflow.decorators import task


@task
def run_pipeline(source_path: str, target_path: str):
    import logging
    
    logging.info(f"Received source_path type: {type(source_path)}")
    
    # 🔴 Force correction if Airflow passes wrong type
    if not isinstance(source_path, str):
        raise ValueError(f"Expected S3 path string but got {type(source_path)}")

    return convert_s3_to_delta(source_path, target_path)