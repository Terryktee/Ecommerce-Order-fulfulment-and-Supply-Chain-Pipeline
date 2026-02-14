from .write_delta import write_table
from airflow.decorators import task

@task
def run_pipeline(df, base_path):
    write_table(df, base_path, "supply_chain_order_fulfulment", partition_by=None)