import base64
import json
import unittest

import numpy as np
import pandas as pd
import requests
from azure.storage.blob import BlobBlock

import timeseriesclient
from timeseriesclient.rest_api import FilesAPI
from timeseriesclient.rest_api.files import (AzureBlobService,
                                             AzureException)

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock


response_upload = """{
  "FileId": "a file id",
  "Account": "the account",
  "SasKey": "the key",
  "Container": "the container",
  "Path": "the name",
  "Endpoint": "the endpoint"
}"""


def make_upload_response():
    response = requests.Response()
    response._content = response_upload.encode('ascii')
    return response


def make_status_response():
    response = requests.Response()
    response._content = r'{"State":"Ready"}'.encode('ascii')
    return response


def setUpModule():
    timeseriesclient.globalsettings.environment.set_qa()


class Test_FilesAPI(unittest.TestCase):

    def setUp(self):
        self.token = {'accessToken': 'abcdef'}
        self.dummy_header = {'Authorization': 'Bearer abcdef'}

        self.api = FilesAPI()
        self.api._session = Mock()

    @patch('timeseriesclient.rest_api.files.TokenAuth')
    def test_upload(self, mock_token):
        mock_post = self.api._session.post

        response = Mock()
        response_upload = {"test": "abs"}
        response.json.return_value = response_upload
        mock_post.return_value = response

        result = self.api.upload(self.token)

        mock_post.assert_called_once_with(
            'https://reservoir-api-qa.4subsea.net/api/Files/upload',
            auth=mock_token(), **self.api._defaults)

        self.assertIsInstance(result, dict)
        self.assertEqual(result, response_upload)

    @patch('timeseriesclient.rest_api.files.TokenAuth')
    def test_commit(self, mock_token):
        mock_post = self.api._session.post

        self.api.commit(self.token, 'fileid')

        mock_post.assert_called_with('https://reservoir-api-qa.4subsea.net/api/Files/commit',
                                     data={'FileId': 'fileid'}, auth=mock_token(),
                                     **self.api._defaults)

    @patch('timeseriesclient.rest_api.files.TokenAuth')
    def test_status(self, mock_token):
        mock_get = self.api._session.get

        response = Mock()
        response.text = '{"test": "abc"}'

        mock_get.return_value = response
        self.api.status(self.token, 'fileid')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/files/fileid/status'
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    **self.api._defaults)

    @patch('timeseriesclient.rest_api.files.TokenAuth')
    def test_bytes(self, mock_token):
        mock_get = self.api._session.get

        self.api.bytes(self.token, 'fileid')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/files/fileid/bytes'
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    **self.api._defaults)

    @patch('timeseriesclient.rest_api.files.AzureBlobService')
    def test_upload_service(self,  mock_df_uploader):
        api = FilesAPI()

        api.upload_service({'abc': '123'})
        mock_df_uploader.assert_called_once_with({'abc': '123'})

    @patch('timeseriesclient.rest_api.files.AzureBlobDownloadService')
    def test_download_service(self,  mock_df_downloader):
        api = FilesAPI()

        api.download_service({'abc': '123'})
        mock_df_downloader.assert_called_once_with({'abc': '123'})


class Test_DataFrameUploader(unittest.TestCase):

    def setUp(self):
        self.upload_params = {'Account': 'account_xyz',
                              'Container': 'blobcontainer', 'Path': 'blob_xy',
                              'FileId': 'file_123abc', 'SasKey': 'sassykeiz'}

    def test_constructor(self):
        uploader = AzureBlobService(self.upload_params)

        expected_attributes = ['container_name', 'blob_name', 'file_id']

        for attribute in expected_attributes:
            if not hasattr(uploader, attribute):
                self.fail(
                    'Expected uploader to have attribute {}'.format(attribute))

    def test_upload(self):
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock()
        uploader.put_block_list = Mock()

        df = pd.DataFrame({'a': np.arange(1001)})
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

        timevector = np.array(np.arange(0, 1001e9, 1e9),
                              dtype='datetime64[ns]')
        df = pd.DataFrame({'a': np.arange(1001)}, index=timevector)
        uploader.create_blob_from_df(df)

        df.index = df.index.astype(np.int64)
        uploader.put_block.assert_called_once_with(self.upload_params['Container'],
                                                   self.upload_params['Path'],
                                                   df.to_csv(header=False),
                                                   base64.b64encode('00000000'.encode('ascii')).decode('ascii'))
        uploader.put_block_list.assert_called_once()

    def test_upload_long(self):
        side_effect = 4 * [None]
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        df = pd.DataFrame({'a': np.arange(1.001e6)})
        uploader.create_blob_from_df(df)

        # 4 was just found empirically + 1 error
        self.assertEqual(uploader.put_block.call_count, 4)

    @patch('timeseriesclient.rest_api.files.sleep')
    def test_upload_long_w_azureexception(self, mock_sleep):
        side_effect = 3 * [AzureException] + 4 * [None]
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        df = pd.DataFrame({'a': np.arange(1.001e6)})
        uploader.create_blob_from_df(df)

        # 4 was just found empirically + 3 error
        self.assertEqual(uploader.put_block.call_count, 4 + 3)

    @patch('timeseriesclient.rest_api.files.sleep')
    def test_upload_raise_azureexception(self, mock_sleep):
        side_effect = 6 * [AzureException] + 4 * [None]
        uploader = AzureBlobService(self.upload_params)
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        df = pd.DataFrame({'a': np.arange(1.001e6)})

        with self.assertRaises(AzureException):
            uploader.create_blob_from_df(df)

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

        df = pd.DataFrame({'a': np.arange(999)})

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
