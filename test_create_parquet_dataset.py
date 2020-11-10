import unittest
import pandas as pd
import os.path
from create_parquet_dataset import *


class TestCreateParquetDataset(unittest.TestCase):

        def setUp(self):
            self.input_file = 'data/btcusd.csv'
            self.output_file = 'data/test.parquet'
            self.test_columns = ['time', 'open', 'close', 'high', 'low', 'volume']

        def tearDown(self):

            for f in os.listdir('data/'):
                if not f.endswith('.parquet'):
                    continue
                os.remove(os.path.join('data/', f))

        def test_if_parquet_file_is_created(self):

                create_parquet_dataset(self.input_file, self.output_file)
                self.assertTrue(os.path.isfile(self.output_file))

        def test_if_parquet_file_contains_all_columns(self):

            create_parquet_dataset(self.input_file, self.output_file)

            df = pq.read_pandas(self.output_file).to_pandas()
            self.assertListEqual(df.columns.tolist(), self.test_columns)

        def test_version_in_meta_data_of_parquet_file(self):

            for iver in ['1.0', '2.0']:
                create_parquet_dataset(self.input_file, self.output_file, ver=iver)

                parquet_file = pq.ParquetFile(self.output_file)
                self.assertEqual(parquet_file.metadata.format_version, iver)

        def test_creating_a_dataset(self):
            print('test not implemeted yet')
