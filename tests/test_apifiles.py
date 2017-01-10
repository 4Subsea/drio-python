import unittest
import json

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock

import requests

import timeseriesclient
from timeseriesclient.rest_api import FilesApi

timeseriesclient.globalsettings.environment.set_qa()


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


class Test_FilesApi(unittest.TestCase):

    def setUp(self):
        self.token = { 'accessToken' : 'abcdef' }
        self.dummy_header = {'Authorization': 'Bearer abcdef'}

    @patch('timeseriesclient.rest_api.base_api.requests.post')
    def test_upload(self, mock_post):
        api = FilesApi()

        response = Mock()
        response.text = '{"test": "abs"}'
        response.text = response_upload
        mock_post.return_value = response

        result = api.upload(self.token)

        mock_post.assert_called_once_with(
            'https://reservoir-api-qa.4subsea.net/api/Files/upload',
            headers=self.dummy_header)

        self.assertIsInstance(result, dict)
        self.assertEqual(result, json.loads(response_upload) )

    @patch('timeseriesclient.rest_api.base_api.requests.post')
    def test_commit(self, mock_post):
        api = FilesApi()
        api.commit(self.token, 'fileid')

        mock_post.assert_called_with('https://reservoir-api-qa.4subsea.net/api/Files/commit', data={'FileId': 'fileid'}, headers={'Authorization': 'Bearer abcdef'})

    @patch('timeseriesclient.rest_api.base_api.requests.get')
    def test_status(self, mock_get):
        api = FilesApi()

        response = Mock()
        response.text = '{"test": "abc"}'

        mock_get.return_value = response
        api.status(self.token, 'fileid')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/files/fileid/status'
        mock_get.assert_called_with(expected_uri, headers=self.dummy_header);

    @patch('timeseriesclient.rest_api.files_api.AzureBlobService')
    def test_upload_service(self,  mock_df_uploader):
        api = FilesApi()

        api.upload_service({'abc': '123'})
        mock_df_uploader.assert_called_once_with({'abc': '123'})


if __name__ == '__main__':
    unittest.main()
