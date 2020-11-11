import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def create_parquet_dataset(input_file, output_file, ver='1.0', split=False):

    df = pd.read_csv(input_file)

    if split is False:

        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, version=ver)

    elif split is True:

        step_size = 100000
        n = 1
        for i in range(0, df.shape[0] - step_size, step_size):
            table = pa.Table.from_pandas(df.iloc[i:i + step_size])

            tmp_output_file = '{}_{}.{}'.format(
                output_file.split('.')[0],
                str(n).zfill(3),
                output_file.split('.')[1])

            pq.write_table(table, tmp_output_file, version=ver)
            n += 1
