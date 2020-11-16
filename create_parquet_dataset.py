import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import sys


def create_parquet_dataset(input_file, output_file, ver='1.0', split=False, add_random=False):

    df = pd.read_csv(input_file)

    if split is False:

        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, version=ver)

    elif split is True:

        step_size = 100000

        n_files = round(df.shape[0] / step_size)
        random_file = np.random.randint(1, n_files)

        n = 1
        for i in range(0, df.shape[0] - step_size, step_size):

            if n == random_file and add_random == True:
                df['random_column'] = 0

            table = pa.Table.from_pandas(df.iloc[i:i + step_size])

            tmp_output_file = '{}_{}.{}'.format(
                output_file.split('.')[0],
                str(n).zfill(3),
                output_file.split('.')[1])

            pq.write_table(table, tmp_output_file, version=ver)
            n += 1


if __name__ == "__main__":
    create_parquet_dataset(sys.argv[1], sys.argv[2], ver='1.0', split=True, add_random=False)
