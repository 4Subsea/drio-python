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

    @patch('timeseriesclient.rest_api.base_api.requests.get')
    def test_list(self, mock_post):
        api = TimeSeriesApi()

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = api.list(self.token)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/TimeSeries/list'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_post.assert_called_with(expected_uri, headers=expected_header)

    @patch('timeseriesclient.rest_api.base_api.requests.get')
    def test_info(self, mock_post):
        api = TimeSeriesApi()

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = api.info(self.token, "someId")

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/someId'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_post.assert_called_with(expected_uri, headers=expected_header)

    @patch('timeseriesclient.rest_api.base_api.requests.delete')
    def test_delete(self, mock_delete):
        api = TimeSeriesApi()
        timeseries_id = '123456'

        mock_delete.return_value = Mock()
        mock_delete.return_value.status_code = 200

        result = api.delete(self.token, timeseries_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/123456'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_delete.assert_called_with(expected_uri, headers=expected_header)

    @patch('timeseriesclient.rest_api.base_api.requests.post')
    def test_create(self, mock_post):
        api = TimeSeriesApi()
        file_id = 666

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        result = api.create(self.token, file_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/create'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        expected_body = { "FileId":file_id }

        mock_post.assert_called_with(expected_uri, 
                                     headers=expected_header,
                                     data=expected_body)


    @patch('timeseriesclient.rest_api.base_api.requests.post')
    def test_add(self, mock_post):
        api = TimeSeriesApi()
        timeseries_id = 't666'
        file_id = 'f001'

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'
        
        result = api.add(self.token, timeseries_id, file_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/add'
        expected_header = {'Authorization': 'Bearer abcdef'}
        expected_body = {"TimeSeriesId":timeseries_id, "FileId":file_id}

        mock_post.assert_called_with(expected_uri, 
                                     headers=expected_header,
                                     data=expected_body)

    @patch('timeseriesclient.rest_api.base_api.requests.get')
    def test_data(self, mock_get):
        api = TimeSeriesApi()
        timeseries_id = 't666'
        start = -1000
        end = 6660000

        result = api.data(self.token, timeseries_id, start, end)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/timeseries/{ts_id}/data'.format(ts_id=timeseries_id)
        expected_header = {'Authorization': 'Bearer abcdef'}
        expected_params = {'start': start, 'end': end}

        mock_get.assert_called_with(expected_uri, 
                                    headers=expected_header,
                                    params=expected_params)


if __name__ == '__main__':
    unittest.main()
