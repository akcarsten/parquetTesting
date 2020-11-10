import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def create_parquet_dataset(input_file, output_file):

    df = pd.read_csv(input_file)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)
