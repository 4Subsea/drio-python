import unittest
import requests
import json
import sys

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock

sys.path.append('../../')

from timeseriesclient.apitimeseries import TimeSeriesApiMock, TimeSeriesApi

class Test_TimeSeriesApi(unittest.TestCase):

    def setUp(self):
        self.token = { 'accessToken' : 'abcdef' }

    @patch('timeseriesclient.apitimeseries.requests.get')
    def test_list(self, mock_post):
        api = TimeSeriesApi()

        example_response = """[
              { "FileId": 0,
                "TimeOfFirstSample": 0,
                "TimeOfLastSample": 0,
                "TimeSeriesId": 0,
                "FileStatus": 0,
                "ReferenceTime": "2016-11-28T08:25:16.184Z"
              }
            ]"""

        response = requests.Response()
        response._content = example_response.encode('ascii')
        mock_post.return_value = response

        result = api.list(self.token)

        expected_uri = 'https://reservoir-api-qa.azurewebsites.net/api/TimeSeries/list'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_post.assert_called_with(expected_uri, headers=expected_header)

        self.assertEqual(result, json.loads(example_response))

    @patch('timeseriesclient.apitimeseries.requests.get')
    def test_delete(self, mock_post):
        api = TimeSeriesApi()
        timeseries_id = '123456'

        example_response = """{ "StatusCode": 100, "Request": {} }"""

        response = requests.Response()
        response._content = example_response.encode('ascii')
        mock_post.return_value = response

        result = api.delete(self.token, timeseries_id)

        expected_uri = 'https://reservoir-api-qa.azurewebsites.net/api/TimeSeries/delete/123456'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_post.assert_called_with(expected_uri, headers=expected_header)

        #self.assertEqual(result, json.loads(example_response))


class Test_TimeSeriesApiMock(unittest.TestCase):

    def test_list_return_value(self):
        api = TimeSeriesApiMock()
        result = api.list(token={})
        expected = ['ts1', 'ts2', 'ts3']
        self.assertEqual(result, expected)

    def test_list_token_stored(self):
        api = TimeSeriesApiMock()
        token = {'key':'supervalue'}
        api.list(token)

        self.assertEqual(api.last_token, token)

        
