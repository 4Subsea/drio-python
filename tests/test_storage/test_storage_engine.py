import unittest
import numpy as np
import pandas as pd
import requests

from azure.storage.blob import BlobBlock
from datareservoirio.storage.storage_engine import AzureBlobService, AzureException

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock


class Test_AzureBlobService(unittest.TestCase):
    def setUp(self):
        self.upload_params = {'Account': 'account_xyz',
                              'Container': 'blobcontainer', 'Path': 'blob_xy',
                              'FileId': 'file_123abc', 'SasKey': 'sassykeiz'}

    def test_constructor(self):
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            expected_attributes = ['container_name', 'blob_name']
            for attribute in expected_attributes:
                if not hasattr(uploader, attribute):
                    self.fail(
                        'Expected uploader to have attribute {}'.format(attribute))

    def test_upload(self):
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            uploader.put_block = Mock()
            uploader.put_block_list = Mock()

            series = pd.Series(np.arange(500))
            uploader.create_blob_from_series(series)

            uploader.put_block.assert_called_once_with(self.upload_params['Container'],
                                                       self.upload_params['Path'],
                                                       series.to_csv(header=False).encode('ascii'),
                                                       'MDAwMDAwMDA=')
            uploader.put_block_list.assert_called_once()

    def test_upload_converts_datetime64_to_int64(self):
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            uploader.put_block = Mock()
            uploader.put_block_list = Mock()

            timevector = np.array(np.arange(0, 1001e9, 1e9),
                                  dtype='datetime64[ns]')
            series = pd.Series(np.arange(1001), index=timevector)
            uploader.create_blob_from_series(series)

            series.index = series.index.astype(np.int64)
            uploader.put_block.assert_called_once_with(self.upload_params['Container'],
                                                       self.upload_params['Path'],
                                                       series.to_csv(header=False).encode('ascii'),
                                                       'MDAwMDAwMDA=')
            uploader.put_block_list.assert_called_once()

    def test_upload_long(self):
        side_effect = 4 * [None]
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            uploader.put_block = Mock(side_effect=side_effect)
            uploader.put_block_list = Mock()

            series = pd.Series(np.arange(1.001e6))
            uploader.create_blob_from_series(series)

            # 4 was just found empirically + 1 error
            self.assertEqual(uploader.put_block.call_count, 4)

    @patch('datareservoirio.storage.storage_engine.sleep')
    def test_upload_long_w_azureexception(self, mock_sleep):
        side_effect = 3 * [AzureException] + 4 * [None]
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            uploader.put_block = Mock(side_effect=side_effect)
            uploader.put_block_list = Mock()

            series = pd.Series(np.arange(1.001e6))
            uploader.create_blob_from_series(series)

            # 4 was just found empirically + 3 error
            self.assertEqual(uploader.put_block.call_count, 4 + 3)

    @patch('datareservoirio.storage.storage_engine.sleep')
    def test_upload_raise_azureexception(self, mock_sleep):
        side_effect = 6 * [AzureException] + 4 * [None]
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            uploader.put_block = Mock(side_effect=side_effect)
            uploader.put_block_list = Mock()

            series = pd.Series(np.arange(1.001e6))

            with self.assertRaises(AzureException):
                uploader.create_blob_from_series(series)

    def test_make_block(self):
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            block = uploader._make_block(0)

            self.assertIsInstance(block, BlobBlock)
            self.assertEqual(block.id, 'MDAwMDAwMDA=')

    def test_b64encode(self):
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            b64_result = uploader._b64encode(0)
            b64_expected = 'MDAwMDAwMDA='
            self.assertEqual(b64_result, b64_expected)

            b64_result = uploader._b64encode(1)
            b64_expected = 'MDAwMDAwMDE='
            self.assertEqual(b64_result, b64_expected)

            b64_result = uploader._b64encode(int(1e6))
            b64_expected = 'MDEwMDAwMDA='
            self.assertEqual(b64_result, b64_expected)

    def test_gen_line_chunks(self):
        with requests.Session() as s:
            uploader = AzureBlobService(self.upload_params, session=s)
            df = pd.Series(np.arange(999))

            for i, chunk in enumerate(uploader._gen_line_chunks(df, 100)):
                if i < 9:
                    self.assertEqual(len(chunk), 100)
                elif i == 9:
                    self.assertEqual(len(chunk), 99)
                else:
                    self.fail("Too many iterations")

            self.assertEqual(i + 1, 10)


if __name__ == '__main__':
    unittest.main()
