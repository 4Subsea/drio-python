import unittest
import requests
import json 
import sys

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock

sys.path.append('../../')
from timeseriesclient.apifiles import FilesApi, FilesApiMock


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


class Test_FilesApi(unittest.TestCase):

    def setUp(self):
        self.token = { 'accessToken' : 'abcdef' }

    @patch('timeseriesclient.apifiles.requests.post')
    def test_upload(self, mock_post):
        api = FilesApi()
        mock_post.return_value = make_upload_response()

        result = api.upload(self.token)

        mock_post.assert_called_once_with(
            'https://reservoir-api-qa.azurewebsites.net/api/Files/upload',
            headers={'Authorization': 'Bearer abcdef'}, data=None)

        self.assertIsInstance(result, dict)
        self.assertEqual(result, json.loads(response_upload) )

    @patch('timeseriesclient.apifiles.requests.post')
    def test_commit(self, mock_post):
        api = FilesApi()

        api.commit(self.token, 'fileid')

        mock_post.assert_called_with('https://reservoir-api-qa.azurewebsites.net/api/Files/commit', data={'FileId': 'fileid'}, headers={'Authorization': 'Bearer abcdef'})

        
class Test_FilesApiMock(unittest.TestCase):

    def test_(self):
        api = FilesApiMock()
