import unittest
import json

try:
    from unittest.mock import Mock, MagicMock, patch, PropertyMock
except:
    from mock import Mock, MagicMock, patch, PropertyMock

import requests
import pandas as pd
import numpy as np

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

        self.assertIsInstance(client._timeseries_api, TimeSeriesApi)


class TestTimeSeriesClient_Authenticate(unittest.TestCase):

    def test_authenticate_returns_correct_token(self):
        client = timeseriesclient.TimeSeriesClient()

        dummy_token = {'accessToken' : 'abcdef',
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
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

        dummy_token = {'accessToken' : 'abcdef',
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
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
        dummy_token ={'accessToken' : 'dummyToken' ,
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        client._authenticator._token = dummy_token
        client._files_api = FilesApiMock()

        response = client.ping()

        self.assertEqual(response, {'status':'pong'})


class Test_Create(unittest.TestCase):

    def setUp(self):
        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }

        self.dummy_token = {'accessToken' : 'abcdef',
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        self.dummy_df = pd.DataFrame({'a':np.arange(1e3)}, index=np.array(np.arange(1e3), dtype='datetime64[ns]'))

        self.create_response = {"TimeSeriesId":'tsfromhell'}

        self.client = timeseriesclient.TimeSeriesClient()
        self.client._authenticator._token = self.dummy_token
        self.client._files_api.commit = Mock(return_value=200)
        self.client._timeseries_api.create = Mock(return_value=self.create_response)
        self.client._files_api.upload = Mock(return_value=self.dummy_params)
        self.client._verify_and_prepare_dataframe = Mock()
        self.client._get_reference_time = Mock(return_value='1970-01-01T00:00:00')
        self.client._get_file_status = Mock(side_effect=["Processing", "Processing", "Ready"])

    @patch('timeseriesclient.storage.get_blobservice')
    def test_check_arguments_called(self, mock_blob):
        self.client.create(self.dummy_df)
        self.client._verify_and_prepare_dataframe.assert_called_with(self.dummy_df)

    @patch('timeseriesclient.storage.get_blobservice')
    def test_all_calls_made(self, mock_blobservice):
        df = pd.DataFrame({'a':np.arange(1e3)})
        self.client.create(df)

        self.client._files_api.upload.assert_called_with(self.dummy_token)
        mock_blobservice.asser_called_with()
        self.client._files_api.commit.assert_called_with(self.dummy_token, self.dummy_params['FileId'])
        self.assertTrue(self.client._timeseries_api.create.called)

    @patch('timeseriesclient.storage.get_blobservice')
    def test_create_file_endpoint_called(self, mock_blob):
        result = self.client.create(self.dummy_df)

        exp_id = 666

        self.client._timeseries_api.create.assert_called_once_with(self.dummy_token,
                                                              exp_id)

        self.assertEqual(result, {"TimeSeriesId":'tsfromhell'})


class Test_Append(unittest.TestCase):

    def setUp(self):
        self.client = timeseriesclient.TimeSeriesClient()

        self.file_id = 'file123abc'

        self.client._verify_and_prepare_dataframe = Mock()
        self.client._upload_file = Mock(return_value=self.file_id)
        self.client._wait_until_file_ready = Mock(side_effect=["Processing", "Processing", "Ready"])

        self.dummy_df = pd.DataFrame({'a':np.arange(10)}, index=np.array(np.arange(10), dtype='datetime64[ns]'))

        self.client._timeseries_api = Mock()

        self.dummy_token = {'accessToken' : 'abcdef',
                            'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        self.client._authenticator = Mock()
        self.client._authenticator.token = self.dummy_token

    def test_append(self):
        self.client._timeseries_api.add.return_value = {'response': 123}
        timeseries_id = 't666'

        result = self.client.append(self.dummy_df, timeseries_id)

        self.client._wait_until_file_ready.assert_called_once_with(self.file_id)
        self.client._verify_and_prepare_dataframe.assert_called_once_with(self.dummy_df)

        self.client._timeseries_api.add.assert_called_once_with(self.dummy_token,
                                                                timeseries_id,
                                                                self.file_id)
        self.assertEqual(result, {'response': 123})


class Test_CheckArgumentsCreate(unittest.TestCase):
    def setUp(self):
        self.client = timeseriesclient.TimeSeriesClient()

    def make_dataframe_datetime64(self):
        timevector = np.array(np.arange(0, 10e9, 1e9), dtype='datetime64[ns]')
        values = np.random.rand(10)
        return  pd.DataFrame({'values' : values}, index=timevector)

    def make_dataframe_int64(self):
        timevector = np.arange(0, 10e9, 1e9, dtype=np.int64)
        values = np.random.rand(10)
        return  pd.DataFrame({'values' : values}, index=timevector)

    def test_datetime64(self):
        df = self.make_dataframe_datetime64()
        result = self.client._verify_and_prepare_dataframe(df)
        self.assertIsNone(result)

    def test_int64(self):
        df = self.make_dataframe_int64()
        result = self.client._verify_and_prepare_dataframe(df)
        self.assertIsNone(result)

    def test_not_a_dataframe(self):
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_dataframe('this is wrong input')

    def test_too_many_columns(self):
        df = self.make_dataframe_datetime64()
        df['values2'] = df['values'].values

        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_dataframe(df)


class Test_ListTimeSeries(unittest.TestCase):

    def test_(self):
        client = timeseriesclient.TimeSeriesClient()
        dummy_token ={'accessToken' : 'dummyToken' ,
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        client._authenticator._token = dummy_token
        client._timeseries_api = TimeSeriesApiMock()

        timeseries = client.list()

        self.assertEqual(timeseries, client._timeseries_api.list_return_value)
        self.assertEqual(client._timeseries_api.last_token, dummy_token)


class Test_Info(unittest.TestCase):
    def test_info(self):
        client = timeseriesclient.TimeSeriesClient()
        dummy_token ={'accessToken' : 'dummyToken' ,
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        client._authenticator._token = dummy_token
        client._timeseries_api = TimeSeriesApiMock()

        result = client.info("someId")

        self.assertIsInstance(result, dict)
        self.assertEqual(result["TimeSeriesId"],"someId") 
        self.assertEqual(client._timeseries_api.last_token, dummy_token)


class Test_DeleteTimeSeries(unittest.TestCase):

    def test_(self):
        client = timeseriesclient.TimeSeriesClient()
        dummy_token ={'accessToken' : 'dummyToken' ,
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        client._authenticator._token = dummy_token
        client._timeseries_api = TimeSeriesApiMock()

        response = client.delete('123456')

        self.assertEqual(client._timeseries_api.last_token, dummy_token)


class Test_Get(unittest.TestCase):

    def setUp(self):
        self.client = timeseriesclient.TimeSeriesClient()

        self.client._timeseries_api = Mock()
    
        self.dummy_token = {'accessToken' : 'abcdef',
                            'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        self.client._authenticator = Mock()
        self.client._authenticator.token = self.dummy_token

    def test_call_timeseries_api_default(self):
        response = Mock()
        response.text = '1,1\n2,2\n3,3\n4,4'

        self.client._timeseries_api.data.return_value = response
        timeseries_id = 't666'
    
        result = self.client.get(timeseries_id)

        start = timeseriesclient.timeseriesclient._START_DEFAULT
        end = timeseriesclient.timeseriesclient._END_DEFAULT

        self.client._timeseries_api.data.assert_called_once_with(
            self.dummy_token, timeseries_id, start, end)

    def test_call_timeseries_api_start(self):
        response = Mock()
        response.text = '1,1\n2,2\n3,3\n4,4'

        self.client._timeseries_api.data.return_value = response
        timeseries_id = 't666'
    
        result = self.client.get(timeseries_id, start='1970-01-01 00:00:10')

        start = 10000000000
        end = timeseriesclient.timeseriesclient._END_DEFAULT

        self.client._timeseries_api.data.assert_called_once_with(
            self.dummy_token, timeseries_id, start, end)

    def test_call_timeseries_api_stop(self):
        response = Mock()
        response.text = '1,1\n2,2\n3,3\n4,4'

        self.client._timeseries_api.data.return_value = response
        timeseries_id = 't666'
    
        result = self.client.get(timeseries_id, end='1970-01-01 00:00:10')

        start = timeseriesclient.timeseriesclient._START_DEFAULT
        end = 10000000000

        self.client._timeseries_api.data.assert_called_once_with(
            self.dummy_token, timeseries_id, start, end)

    def test_convert_to_series_date(self):
        response = Mock()
        response.text = '1,1\n2,2\n3,3\n4,4'

        self.client._timeseries_api.data.return_value = response
        timeseries_id = 't666'
    
        result = self.client.get(timeseries_id, convert_date=True)

        expected = pd.DataFrame([1, 2, 3, 4],
                                columns=['values'], index=[1, 2, 3, 4])
        expected.index.name = 'time'
        expected.index = pd.to_datetime(expected.index, unit ='ns')

        pd.util.testing.assert_series_equal(result, expected['values'])

    def test_convert_to_series_ns(self):
        response = Mock()
        response.text = '1,1\n2,2\n3,3\n4,4'

        self.client._timeseries_api.data.return_value = response
        timeseries_id = 't666'

        result = self.client.get(timeseries_id, convert_date=False)

        expected = pd.DataFrame([1, 2, 3, 4],
                                columns=['values'], index=[1, 2, 3, 4])
        expected.index.name = 'time'

        pd.util.testing.assert_series_equal(result, expected['values'])
