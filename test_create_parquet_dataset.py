import unittest
import pandas as pd
import os.path
import shutil
import zipfile
from create_parquet_dataset import *


class TestCreateParquetDataset(unittest.TestCase):

        def setUp(self):
            self.data_file = 'data/btcusd.zip'
            self.output_path = 'data/tmp'
            self.input_file = '{}/btcusd.csv'.format(self.output_path)
            self.output_file = '{}/test.parquet'.format(self.output_path)
            self.test_columns = ['time', 'open', 'close', 'high', 'low', 'volume']

            os.mkdir('data/tmp')

            with zipfile.ZipFile(self.data_file, 'r') as zip_ref:
                zip_ref.extractall(self.output_path)

        def tearDown(self):
            shutil.rmtree(self.output_path)

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

            create_parquet_dataset(self.input_file, self.output_file, split=True)

            # Remove the .csv file to read the parquet files as a dataset
            os.remove(self.input_file)

            dataset = pq.ParquetDataset(self.output_path)
            table = dataset.read()
            n_files = len(os.listdir(self.output_path))

            self.assertEqual(n_files, 30)
            self.assertEqual(table.to_pandas().shape[0], 3000000)

        def test_add_column_in_random_file(self):

            create_parquet_dataset(self.input_file, self.output_file, split=True, add_random=True)

            # Trying to load a dataset in which one parquet file has an
            # extra column will raise a ValueError
            self.assertRaises(ValueError, pq.ParquetDataset, self.output_path)
