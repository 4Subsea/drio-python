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


  
dummy_response_append = """
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

def make_append_to_response():
    response = requests.Response()
    response._content = dummy_response_append.encode('ascii')

    return response

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

    @patch('timeseriesclient.apitimeseries.requests.delete')
    def test_delete(self, mock_delete):
        api = TimeSeriesApi()
        timeseries_id = '123456'

        example_response = """{ "StatusCode": 100, "Request": {} }"""

        response = requests.Response()
        response._content = example_response.encode('ascii')
        mock_delete.return_value = response

        result = api.delete(self.token, timeseries_id)

        expected_uri = 'https://reservoir-api-qa.azurewebsites.net/api/TimeSeries/delete/123456'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        mock_delete.assert_called_with(expected_uri, headers=expected_header)


    @patch('timeseriesclient.apitimeseries.requests.post')
    def test_create(self, mock_post):
        api = TimeSeriesApi()
        file_id = 666
        reference_time = "1492-10-04"
        
        example_response = """
            {
              "TimeSeriesId" : 0,
              "FileId": 0,
              "TimeOfFirstSample": 0,
              "TimeOfLastSample": 0,
              "TimeSeriesId": 0,
              "FileStatus": 0,
              "ReferenceTime": "2016-11-29T06:41:37.968Z"
            }"""

        response = requests.Response()
        response._content = example_response.encode('ascii')
        mock_post.return_value = response

        result = api.create(self.token, file_id, reference_time)

        expected_uri = 'https://reservoir-api-qa.azurewebsites.net/api/TimeSeries/create'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        expected_body = { "FileId":file_id, "ReferenceTime":reference_time }

        mock_post.assert_called_with(expected_uri, 
                                     headers=expected_header,
                                     data=expected_body)

        self.assertEqual(result, json.loads(example_response))

    @patch('timeseriesclient.apitimeseries.requests.post')
    def test_append(self, mock_post):
        api = TimeSeriesApi()
        timeseries_id = 't666'
        file_id = 'f001'

        mock_post.return_value = make_append_to_response()

        result = api.append(self.token, timeseries_id, file_id)

        expected_uri = 'https://reservoir-api-qa.azurewebsites.net/api/TimeSeries/append_to'
        expected_header = { 'Authorization' : 'Bearer abcdef' }
        expected_body = { "TimeSeriesId":timeseries_id, "FileId":file_id }

        mock_post.assert_called_with(expected_uri, 
                                     headers=expected_header,
                                     data=expected_body)

        self.assertEqual(result, json.loads(dummy_response_append))
        

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

    def test_delete_token_stored(self):
        api = TimeSeriesApiMock()
        token = {'key':'deletetoken'}
        api.delete(token, '123456')

        self.assertEqual(api.last_token, token)

    def test_create_return_value(self):
        api = TimeSeriesApiMock()
        result = api.create({}, 0, "1970")
        expected = { "TimeSeriesId", 'abcde' }
 
        self.assertEqual(result, expected)

    def test_create_token_stored(self):
        api = TimeSeriesApiMock()
        token = {'key':'createtoken'}
        api.create(token, 0, "1970")
 
        self.assertEqual(api.last_token, token)
        
