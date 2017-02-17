import unittest
import base64

try:
    from unittest.mock import Mock, patch
except:
    from mock import Mock, patch

import pandas as pd
import numpy as np
from azure.storage.blob import BlobBlock

from timeseriesclient.rest_api.files_api import AzureBlobService, AzureException


class Test_DataFrameUploader(unittest.TestCase):

    def setUp(self):
        self.upload_params = {'Account': 'account_xyz',
                              'Container':'blobcontainer', 'Path':'blob_xy',
                              'FileId': 'file_123abc', 'SasKey': 'sassykeiz'}

    def test_constructor(self):
        uploader = AzureBlobService(self.upload_params)

        expected_attributes = ['container_name', 'blob_name', 'file_id']

        for attribute in expected_attributes:
            if not hasattr(uploader, attribute):
                self.fail('Expected uploader to have attribute {}'.format(attribute))

    def test_upload(self):
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock()
        uploader.put_block_list = Mock()

        df = pd.DataFrame({'a':np.arange(1001)})
        uploader.create_blob_from_df(df)

        uploader.put_block.assert_called_once_with(self.upload_params['Container'],
                                                   self.upload_params['Path'],
                                                   df.to_csv(header=False),
                                                   base64.b64encode('00000000'.encode('ascii')).decode('ascii'))
        uploader.put_block_list.assert_called_once()

    def test_upload_converts_datetime64_to_int64(self):
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock()
        uploader.put_block_list = Mock()

        timevector = np.array( np.arange(0, 1001e9, 1e9), dtype='datetime64[ns]')
        df = pd.DataFrame({'a':np.arange(1001)}, index=timevector)
        uploader.create_blob_from_df(df)

        df.index = df.index.astype(np.int64)
        uploader.put_block.assert_called_once_with(self.upload_params['Container'],
                                                   self.upload_params['Path'],
                                                   df.to_csv(header=False),
                                                   base64.b64encode('00000000'.encode('ascii')).decode('ascii'))
        uploader.put_block_list.assert_called_once()

    def test_upload_long(self):
        side_effect = [AzureException] + 4*[None]
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        df = pd.DataFrame({'a':np.arange(1.001e6)})
        uploader.create_blob_from_df(df)

        # 4 was just found empirically + 1 error
        self.assertEqual(uploader.put_block.call_count, 4+1)

    def test_make_block(self):
        uploader = AzureBlobService(self.upload_params)

        block = uploader._make_block(0)

        self.assertIsInstance(block, BlobBlock)
        self.assertEqual(block.id, 'MDAwMDAwMDA=')

    def test_b64encode(self):
        uploader = AzureBlobService(self.upload_params)

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
        uploader = AzureBlobService(self.upload_params)

        df = pd.DataFrame({'a':np.arange(999)})

        for i,chunk in enumerate(uploader._gen_line_chunks(df, 100)):
            if i<9:
                self.assertEqual(len(chunk), 100)
            elif i==9:
                self.assertEqual(len(chunk), 99)
            else:
                self.fail("Too many iterations")

        self.assertEqual(i+1, 10)


if __name__ == '__main__':
    unittest.main()
