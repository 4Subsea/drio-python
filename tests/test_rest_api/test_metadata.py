import unittest

import requests

import datareservoirio
from datareservoirio.rest_api import MetadataAPI

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock


dummy_meta = {
    "Namespace": "string",
    "Key": "string",
    "Value": {}
}

dummy_response = """
    {
    "Id": "string",
    "Namespace": "string",
    "Key": "string",
    "Value": {},
    "TimeSeries": [
        {
        "TimeSeriesId": "string",
        "TimeOfFirstSample": 0,
        "TimeOfLastSample": 0,
        "LastModifiedByEmail": "string",
        "Created": "2017-03-01T13:28:39.180Z",
        "LastModified": "2017-03-01T13:28:39.180Z",
        "CreatedByEmail": "string",
        "Metadata": [
            {}
        ]
        }
    ],
    "LastModifiedByEmail": "string",
    "LastModified": "2017-03-01T13:28:39.180Z",
    "Created": "2017-03-01T13:28:39.180Z",
    "CreatedByEmail": "string"
    }"""


def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_MetadataAPI(unittest.TestCase):

    def setUp(self):
        self.token = {'accessToken': 'abcdef'}
        self.metadata_id = 'metadata_id'

        self.api = MetadataAPI()
        self.api._session = Mock()

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_namespacekeys_with_namespaces_returns_list(self, mock_token):
        mock_get = self.api._session.get
        mock_get.return_value = Mock()
        mock_get.return_value.text = '["system.reservoir", "system.campaigns"]'

        self.api.namespacekeys(self.token)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/namespacekeys'

        mock_get.assert_called_once_with(expected_uri,
                                         auth=mock_token(),
                                         **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_get(self, mock_token):
        mock_post = self.api._session.get

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = self.api.get(self.token, self.metadata_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}'.format(
            self.metadata_id)

        mock_post.assert_called_once_with(expected_uri, auth=mock_token(),
                                          **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_list(self, mock_token):
        mock_post = self.api._session.get

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = self.api.list(self.token)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/list'

        mock_post.assert_called_once_with(expected_uri, auth=mock_token(),
                                          **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_create(self, mock_token):
        mock_post = self.api._session.post

        result = self.api.create(self.token, dummy_meta)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/create'

        expected_body = dummy_meta

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=expected_body, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_update(self, mock_token):
        mock_post = self.api._session.put

        result = self.api.update(self.token, self.metadata_id, dummy_meta)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}'.format(
            self.metadata_id)

        expected_body = dummy_meta

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=expected_body, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_delete(self, mock_token):
        mock_post = self.api._session.delete

        result = self.api.delete(self.token, self.metadata_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}'.format(
            self.metadata_id)

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_search(self, mock_token):
        search_json = {
            "Namespace": "hello",
            "Key": "world",
            "Conjunctive": True
        }
        mock_post = self.api._session.post

        result = self.api.search(self.token, search_json)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/search'

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=search_json, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_attach_series(self, mock_token):
        series_list = ['series_1', 'series_2']
        mock_post = self.api._session.post

        result = self.api.attach_series(self.token, self.metadata_id,
                                        series_list)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}/attachTimeSeries'.format(self.metadata_id)

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=series_list, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_detach_series(self, mock_token):
        series_list = ['series_1', 'series_2']
        mock_delete = self.api._session.delete

        result = self.api.detach_series(self.token, self.metadata_id,
                                        series_list)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}/detachTimeSeries'.format(self.metadata_id)

        mock_delete.assert_called_with(expected_uri, auth=mock_token(),
                                       json=series_list, **self.api._defaults)


if __name__ == '__main__':
    unittest.main()
