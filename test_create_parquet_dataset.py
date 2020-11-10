import unittest
import pandas as pd
import os.path
from create_parquet_dataset import *


class TestCreateParquetDataset(unittest.TestCase):

        def setUp(self):
            self.test_csv_file = 'data/btcusd.csv'
            self.test_parquet_file = 'data/test.parquet'
            self.test_columns = ['time', 'open', 'close', 'high', 'low', 'volume']


        def tearDown(self):
            os.remove(self.test_parquet_file)

        def test_if_parquet_file_is_created(self):

                create_parquet_dataset(self.test_csv_file)
                self.assertTrue(os.path.isfile(self.test_parquet_file))

        def test_if_parquet_file_contains_all_columns(self):

            create_parquet_dataset(self.test_csv_file)
            df = pq.read_pandas(self.test_parquet_file).to_pandas()

            self.assertListEqual(df.columns.tolist(), self.test_columns)
