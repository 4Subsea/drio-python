import unittest
import sys
import requests
import json 
#from testfixtures import compare
import pandas as pd
import numpy as np

try:
    from unittest.mock import Mock, MagicMock, patch, PropertyMock
except:
    from mock import Mock, MagicMock, patch, PropertyMock

sys.path.append('../../')
import timeseriesclient
from timeseriesclient.adalwrapper import Authenticator
import timeseriesclient.globalsettings as gs
from timeseriesclient.apitimeseries import TimeSeriesApiMock, TimeSeriesApi
from timeseriesclient.apifiles import FilesApiMock

timeseriesclient.globalsettings.environment.set_qa()

class TestTimeSeriesClient(unittest.TestCase):
    
    def test_constructor(self):
        client = timeseriesclient.TimeSeriesClient()
        
        self.assertIsInstance(client, 
            timeseriesclient.timeseriesclient.TimeSeriesClient)

        self.assertIsInstance(client._authenticator, Authenticator)

        self.assertEqual(client._api_base_url, gs.environment.api_base_url)
        self.assertIsInstance(client._timeseries_api, TimeSeriesApi)

            
class TestTimeSeriesClient_Authenticate(unittest.TestCase):

    def test_authenticate_returns_correct_token(self):
        client = timeseriesclient.TimeSeriesClient()

        dummy_token = {'accessToken' : 'abcdef'}
        client._authenticator._token = dummy_token

        self.assertEqual(client.token, dummy_token)

    def test_authenticate_called(self):
        client = timeseriesclient.TimeSeriesClient()

        client._authenticator.authenticate = Mock(return_value=None)
        
        client.authenticate()

        client._authenticator.authenticate.assert_called_with()


class TestTimeSeriesClient_Token(unittest.TestCase):

    def test_get_token(self):
        client = timeseriesclient.TimeSeriesClient()

        dummy_token = {'accessToken' : 'abcdef'}
        client._authenticator._token = dummy_token 

        self.assertEqual(client.token, dummy_token)

class TestTimeSeriesClient_Ping(unittest.TestCase):

    def setUp(self):
        self._patcher_token = patch('timeseriesclient.TimeSeriesClient.token', 
                                        new_callable=PropertyMock)
        self._mock_token = self._patcher_token.start()
        self._mock_token.return_value = {'accessToken' : 'abcdef'}

    def tearDown(self):
        self._patcher_token.stop()

    def test_ping_request(self):
        client = timeseriesclient.TimeSeriesClient()


        client.ping()

    @patch('timeseriesclient.timeseriesclient.requests.get')
    def test_endpoint_URI(self, mock):
        client = timeseriesclient.TimeSeriesClient()


        expected_uri = client._api_base_url + 'Ping'
        expected_header = { 'Authorization' : 'Bearer abcdef' }

        client.ping()
        
        mock.assert_called_once_with(expected_uri, headers=expected_header)


class Test_UploadTimeseries(unittest.TestCase):

    def setUp(self):
        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }

        self.dummy_token = {'accessToken' : 'abcdef'}

        self.create_response = {"TimeSeriesId":'tsfromhell'}

        self.client = timeseriesclient.TimeSeriesClient()
        self.client._authenticator._token = self.dummy_token
        self.client._files_api.commit = Mock(return_value=200)
        self.client._timeseries_api.create = Mock(return_value=self.create_response)
        self.client._files_api.upload = Mock(return_value=self.dummy_params)
        
        
    @patch('timeseriesclient.storage.get_blobservice')
    def test_all_calls_made(self, mock_blobservice):
        df = pd.DataFrame({'a':np.arange(1e3)})
        self.client.upload_timeseries(df)

        self.client._files_api.upload.assert_called_with(self.dummy_token)
        mock_blobservice.asser_called_with()
        self.client._files_api.commit.assert_called_with(self.dummy_token, self.dummy_params['FileId'])
        self.assertTrue(self.client._timeseries_api.create.called)

    @patch('timeseriesclient.storage.get_blobservice')
    def test_create_file_endpoint_called(self, mock_blob):
        df = pd.DataFrame({'a':np.arange(1e3)})
        result = self.client.upload_timeseries(df)

        exp_time = "1970-01-01T00:00:00"
        exp_id = 666

        self.client._timeseries_api.create.assert_called_once_with(self.dummy_token,
                                                              exp_id,
                                                              exp_time)

        self.assertEqual(result, {"TimeSeriesId":'tsfromhell'})


class Test_ListTimeSeries(unittest.TestCase):

    def test_(self):
        client = timeseriesclient.TimeSeriesClient()
        client._authenticator._token = {'accessToken' : 'dummyToken' }
        client._timeseries_api = TimeSeriesApiMock()

        timeseries = client.list_timeseries()

        self.assertEqual(timeseries, client._timeseries_api.list_return_value)
        self.assertEqual(client._timeseries_api.last_token, {'accessToken' : 'dummyToken' })


class Test_DeleteTimeSeries(unittest.TestCase):

    def test_(self):
        client = timeseriesclient.TimeSeriesClient()
        client._authenticator._token = {'accessToken' : 'dummyToken' }
        client._timeseries_api = TimeSeriesApiMock()

        response = client.delete_timeseries('123456')

        self.assertEqual(client._timeseries_api.last_token, {'accessToken' : 'dummyToken' })

        













