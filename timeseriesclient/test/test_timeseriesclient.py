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


class Test_GetFileUploadParameters(unittest.TestCase):

    def setUp(self):
        self.dummy_params = json.dumps({ 'FileId' : 'a file id',
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }).encode('ascii')


    def create_dummy_response(self):
        response = requests.Response()
        response._content = self.dummy_params
        
        return response
        
    
    @patch('timeseriesclient.timeseriesclient.requests.post')
    def test_calls_api_files_upload(self, post_mock):
        client = timeseriesclient.TimeSeriesClient()
        client._authenticator._token = {'accessToken' : 'dummyToken' }

        post_mock.return_value = self.create_dummy_response()

        client._get_file_upload_params()

        expected_uri = client._api_base_url + 'Files/upload'
        expected_header = { 'Authorization' : 'Bearer dummyToken' }
        post_mock.assert_called_once_with(expected_uri, headers=expected_header)

    @patch('timeseriesclient.timeseriesclient.requests.post')
    def test_returns_python_dictionary(self, post_mock):
        client = timeseriesclient.TimeSeriesClient()
        client._authenticator._token = {'accessToken' : 'dummyToken' }

        post_mock.return_value = self.create_dummy_response()

        upload_params = client._get_file_upload_params()

        self.assertIsInstance(upload_params, dict)
        self.assertEqual(upload_params['FileId'], 'a file id')

class Test_UploadTimeseries(unittest.TestCase):

    def setUp(self):
        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }
        
        
    @patch('timeseriesclient.storage.get_blobservice')
    def test_asks_for_upload_params(self, mock_blobservice):
        client = timeseriesclient.TimeSeriesClient()
        dummy_token = {'accessToken' : 'abcdef'}
        client._authenticator._token = dummy_token
        client._get_file_upload_params = Mock(return_value=self.dummy_params)
        client._commit_file = Mock(return_value=200)
        client._timeseries_api.create = Mock(return_value={"TimeSeriesId":'tsfromhell'})

        df = pd.DataFrame({'a':np.arange(1e3)})
        client.upload_timeseries(df)
        client._get_file_upload_params.assert_called_once_with()


    @patch('timeseriesclient.storage.get_blobservice')
    def test_commits_file(self, mock_blobservice):
        client = timeseriesclient.TimeSeriesClient()
        dummy_token = {'accessToken' : 'abcdef'}
        client._authenticator._token = dummy_token
        client._get_file_upload_params = Mock(return_value=self.dummy_params)
        client._commit_file = Mock(return_value=200)
        client._timeseries_api.create = Mock(return_value={"TimeSeriesId":'tsfromhell'})

        df = pd.DataFrame({'a':np.arange(1e3)})
        client.upload_timeseries(df)

        client._commit_file.assert_called_once_with(666)

    @patch('timeseriesclient.storage.get_blobservice')
    def test_create_file_endpoint_called(self, mock_blob):
        client = timeseriesclient.TimeSeriesClient()
        dummy_token = {'accessToken' : 'abcdef'}
        client._authenticator._token = dummy_token
        client._get_file_upload_params = Mock(return_value=self.dummy_params)
        client._commit_file = Mock(return_value=200)
        client._timeseries_api.create = Mock(return_value={"TimeSeriesId":'tsfromhell'})

        df = pd.DataFrame({'a':np.arange(1e3)})
        result = client.upload_timeseries(df)

        exp_time = "1970-01-01T00:00:00"
        exp_id = 666

        client._timeseries_api.create.assert_called_once_with(dummy_token,
                                                              exp_id,
                                                              exp_time)

        self.assertEqual(result, {"TimeSeriesId":'tsfromhell'})


class Test_UploadFile(unittest.TestCase):

    def test_(self):
        client = timeseriesclient.TimeSeriesClient()
        client._authenticator._token = {'accessToken' : 'dummyToken' }
        

        client.upload_file()
 

class Test_Commitfile(unittest.TestCase):

    @patch('timeseriesclient.timeseriesclient.requests.post')
    def test_(self, mock_post):
        client = timeseriesclient.TimeSeriesClient()
        client._authenticator._token = {'accessToken' : 'dummyToken' }

        client._commit_file('fileid')

        mock_post.assert_called_with('https://reservoir-api.azurewebsites.net/api/Files/commit', data={'FileId': 'fileid'}, headers={'Authorization': 'Bearer dummyToken'})


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

        













