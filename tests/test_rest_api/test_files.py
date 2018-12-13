import unittest

import requests

import datareservoirio
from datareservoirio.rest_api import FilesAPI

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock


response_upload = r"""{
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

        self._session = Mock()
        self.api = FilesAPI(self._session)

    def test_upload(self):
        mock_post = self.api._session.post

        response = Mock()
        response_upload = {'test': 'abs'}
        response.json.return_value = response_upload
        mock_post.return_value = response

        result = self.api.upload()

        mock_post.assert_called_once_with(
            'https://reservoir-api-qa.4subsea.net/api/Files/upload',
            **self.api._defaults)

        self.assertIsInstance(result, dict)
        self.assertEqual(result, response_upload)

    def test_commit(self):
        mock_post = self.api._session.post

        self.api.commit('fileid')

        mock_post.assert_called_with('https://reservoir-api-qa.4subsea.net/api/Files/commit',
                                     data={'FileId': 'fileid'},
                                     **self.api._defaults)

    def test_status(self):
        mock_get = self.api._session.get

        response = Mock()
        response.text = '{"test": "abc"}'

        mock_get.return_value = response
        self.api.status('fileid')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/files/fileid/status'
        mock_get.assert_called_with(expected_uri, **self.api._defaults)


if __name__ == '__main__':
    unittest.main()
