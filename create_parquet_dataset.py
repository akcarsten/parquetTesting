import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def create_parquet_dataset(input_file, output_file, ver='1.0', split=False):

    df = pd.read_csv(input_file)

    if split is False:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, version=ver)
