import unittest
import json

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock

import requests

import timeseriesclient
from timeseriesclient.rest_api import TimeSeriesApi

timeseriesclient.globalsettings.environment.set_qa()


dummy_response_add = """
    {
      "FileId": 0,
      "TimeOfFirstSample": 0,
      "TimeOfLastSample": 0,
      "TimeSeriesId": 0,
      "FileStatus": 0,
      "ReferenceTime": "2016-12-01T11:18:57.481Z",
      "LastModifiedByEmail": "string",
      "Created": "2016-12-01T11:18:57.481Z",
      "LastModified": "2016-12-01T11:18:57.481Z",
      "CreatedByEmail": "string"
    }"""

def make_add_response():
    response = requests.Response()
    response._content = dummy_response_add.encode('ascii')

    return response

class Test_TimeSeriesApi(unittest.TestCase):

    def setUp(self):
        self.token = { 'accessToken' : 'abcdef' }

        self.api = TimeSeriesApi()
        self.api._session = Mock()

    @patch('timeseriesclient.rest_api.timeseries_api.TokenAuth')
    def test_list(self, mock_token):
        mock_post = self.api._session.get

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = self.api.list(self.token)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/TimeSeries/list'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_post.assert_called_once_with(expected_uri, auth=mock_token(), 
                                          **self.api._defaults)

    @patch('timeseriesclient.rest_api.timeseries_api.TokenAuth')
    def test_info(self, mock_token):
        mock_get = self.api._session.get

        mock_get.return_value = Mock()
        mock_get.return_value.text = u'{}'

        result = self.api.info(self.token, "someId")

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/someId'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_get.assert_called_once_with(expected_uri, auth=mock_token(),
                                         **self.api._defaults)

    @patch('timeseriesclient.rest_api.timeseries_api.TokenAuth')
    def test_delete(self, mock_token):
        mock_delete = self.api._session.delete
        timeseries_id = '123456'

        mock_delete.return_value = Mock()
        mock_delete.return_value.status_code = 200

        result = self.api.delete(self.token, timeseries_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/123456'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_delete.assert_called_with(expected_uri, auth=mock_token(),
                                       **self.api._defaults)

    @patch('timeseriesclient.rest_api.timeseries_api.TokenAuth')
    def test_create(self, mock_token):
        file_id = 666

        mock_post = self.api._session.post
        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = self.api.create(self.token, file_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/create'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        expected_body = { "FileId":file_id }

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     data=expected_body, **self.api._defaults)


    @patch('timeseriesclient.rest_api.timeseries_api.TokenAuth')
    def test_add(self, mock_token):
        timeseries_id = 't666'
        file_id = 'f001'

        mock_post = self.api._session.post
        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'
        
        result = self.api.add(self.token, timeseries_id, file_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/add'
        expected_header = {'Authorization': 'Bearer abcdef'}
        expected_body = {"TimeSeriesId":timeseries_id, "FileId":file_id}

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     data=expected_body, **self.api._defaults)

    @patch('timeseriesclient.rest_api.timeseries_api.TokenAuth')
    def test_data(self, mock_token):
        timeseries_id = 't666'
        start = -1000
        end = 6660000

        result = self.api.data(self.token, timeseries_id, start, end)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/{ts_id}/data'.format(ts_id=timeseries_id)
        expected_header = {'Authorization': 'Bearer abcdef'}
        expected_params = {'start': start, 'end': end}

        mock_get = self.api._session.get
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    params=expected_params,
                                    **self.api._defaults)


if __name__ == '__main__':
    unittest.main()
