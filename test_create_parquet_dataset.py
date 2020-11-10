import unittest
import pandas as pd
import os.path
from create_parquet_dataset import *


class TestCreateParquetDataset(unittest.TestCase):

        def setUp(self):
            self.test_csv_file = 'data/btcusd.csv'
            self.test_parquet_file = 'data/test.parquet'

        def tearDown(self):
            os.remove(self.test_parquet_file)

        def test_csv_imported_as_dataframe(self):

                df_result = create_parquet_dataset(self.test_csv_file)
                df_expected = pd.read_csv(self.test_csv_file)

                pd.testing.assert_frame_equal(df_result, df_expected)

                self.assertTrue(os.path.isfile(self.test_parquet_file))
