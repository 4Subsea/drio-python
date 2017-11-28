import json
import unittest

import requests

import datareservoirio
from datareservoirio.rest_api import FilesAPI

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
    datareservoirio.globalsettings.environment.set_qa()


class Test_FilesAPI(unittest.TestCase):

    def setUp(self):
        self.token = {'accessToken': 'abcdef'}
        self.dummy_header = {'Authorization': 'Bearer abcdef'}

        self.api = FilesAPI()
        self.api._session = Mock()

    @patch('datareservoirio.rest_api.files.TokenAuth')
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

    @patch('datareservoirio.rest_api.files.TokenAuth')
    def test_commit(self, mock_token):
        mock_post = self.api._session.post

        self.api.commit(self.token, 'fileid')

        mock_post.assert_called_with('https://reservoir-api-qa.4subsea.net/api/Files/commit',
                                     data={'FileId': 'fileid'}, auth=mock_token(),
                                     **self.api._defaults)

    @patch('datareservoirio.rest_api.files.TokenAuth')
    def test_status(self, mock_token):
        mock_get = self.api._session.get

        response = Mock()
        response.text = '{"test": "abc"}'

        mock_get.return_value = response
        self.api.status(self.token, 'fileid')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/files/fileid/status'
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    **self.api._defaults)

    @patch('datareservoirio.rest_api.files.TokenAuth')
    def test_bytes(self, mock_token):
        mock_get = self.api._session.get

        self.api.bytes(self.token, 'fileid')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/files/fileid/bytes'
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    **self.api._defaults)

    @patch('datareservoirio.rest_api.files.AzureBlobService')
    def test_transfer_service(self,  mock_df_uploader):
        api = FilesAPI()

        api.transfer_service({'abc': '123'})
        mock_df_uploader.assert_called_once_with({'abc': '123'})


if __name__ == '__main__':
    unittest.main()