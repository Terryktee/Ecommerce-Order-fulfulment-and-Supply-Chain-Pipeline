from deltalake import write_deltalake
import pyarrow as pa
import pandas as pd

def write_table(df, base_path, table_name, partition_by=None):
    if isinstance(df, pd.DataFrame):
        table = pa.Table.from_pandas(df, preserve_index=False)
    else:
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    write_deltalake(
        f"{base_path}/{table_name}",
        table,
        mode="overwrite",
        partition_by=partition_by,
    )

