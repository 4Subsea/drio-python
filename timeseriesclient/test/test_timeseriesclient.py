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

timeseriesclient.globalsettings.environment.set_qa()

class TestTimeSeriesClient(unittest.TestCase):
    
    def test_constructor(self):
        client = timeseriesclient.TimeSeriesClient()
        
        self.assertIsInstance(client, 
            timeseriesclient.timeseriesclient.TimeSeriesClient)

        self.assertIsInstance(client._authenticator, Authenticator)

        self.assertEqual(client._api_base_url, gs.environment.api_base_url)

    @patch('timeseriesclient.TimeSeriesClient.token', new_callable=PropertyMock)
    def test_create_authorization_header(self, mock):
        client = timeseriesclient.TimeSeriesClient()

        mock.return_value = {'accessToken' : 'abcdef'}

        key, value = client._create_authorization_header()

        self.assertEqual(key, 'Authorization')
        self.assertEqual(value, 'Bearer abcdef')


    @patch('timeseriesclient.TimeSeriesClient._create_authorization_header')
    def test_add_authorization_header(self, mock):
        client = timeseriesclient.TimeSeriesClient()

        key = 'Authorization'
        value = 'Bearer abcdef'
       
        mock.return_value = (key, value)

        header = client._add_authorization_header({})
        expected_header = { key : value }

        self.assertEqual(header, expected_header)

            
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
        self.dummy_params = { 'FileId' : 'a file id',
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }
        
        
    @patch('timeseriesclient.storage.get_blobservice')
    def test_asks_for_upload_params(self, mock_blobservice):
        client = timeseriesclient.TimeSeriesClient()
        client._get_file_upload_params = Mock(return_value=self.dummy_params)

        df = pd.DataFrame({'a':np.arange(1e3)})
        client.upload_timeseries(df)
        client._get_file_upload_params.assert_called_once_with()



class Test_UploadFile(unittest.TestCase):

    @patch('timeseriesclient.adalwrapper.Authenticator.token', new_callable=PropertyMock)
    def test_(self, mock):
        client = timeseriesclient.TimeSeriesClient()
        
        mock.return_value = 'dummy token'

        client.upload_file()
 




















