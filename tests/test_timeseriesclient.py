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
from timeseriesclient import Authenticator
import timeseriesclient.globalsettings as gs
from timeseriesclient.rest_api import TimeSeriesApi


timeseriesclient.globalsettings.environment.set_qa()


class Test_TimeSeriesClient(unittest.TestCase):

    def setUp(self):
        self.auth = Mock()
        self.auth.token = {'accessToken': 'abcdef',
                           'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}

        self.client = timeseriesclient.TimeSeriesClient(self.auth)
        self.client._files_api = Mock()
        self.client._timeseries_api = Mock()

        self.dummy_df = pd.DataFrame({'a':np.arange(1e3)}, index=np.array(np.arange(1e3), dtype='datetime64[ns]'))

        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }

    def test_init(self):
        self.assertIsInstance(self.client, timeseriesclient.TimeSeriesClient)
        self.assertIsInstance(self.client._authenticator, Mock)
        self.assertIsInstance(self.client._timeseries_api, Mock)
        self.assertIsInstance(self.client._files_api, Mock)

    def test_token(self):
        self.assertEqual(self.client.token, self.auth.token)

    def test_ping_request(self):
        self.client._files_api.ping.return_value = {'status':'pong'}

        response = self.client.ping()
        self.assertEqual(response, {'status':'pong'})

    def test_create_all_methods_called(self):
        self.client._verify_and_prepare_dataframe = Mock(return_value=None)
        self.client._upload_file = Mock(return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.create.return_value = expected_response

        response = self.client.create(self.dummy_df)

        self.client._verify_and_prepare_dataframe.assert_called_once_with(self.dummy_df)
        self.client._upload_file.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(self.dummy_params['FileId'])
        self.client._timeseries_api.create.assert_called_once_with(self.auth.token, self.dummy_params['FileId'])
        self.assertDictEqual(response, expected_response)


class Test_Create(unittest.TestCase):

    def setUp(self):
        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }

        timeseriesclient.rest_api.files_api.AzureBlobService = Mock()
        timeseriesclient.rest_api.files_api.AzureBlobService.file_id = self.dummy_params['FileId']
        timeseriesclient.rest_api.files_api.AzureBlobService.container_name = self.dummy_params['Container']
        timeseriesclient.rest_api.files_api.AzureBlobService.blob_name = self.dummy_params['Path']
        self.dummy_token = {'accessToken' : 'abcdef',
                            'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's')}
        auth = Mock()
        auth.token = self.dummy_token

        self.dummy_df = pd.DataFrame({'a':np.arange(1e3)}, index=np.array(np.arange(1e3), dtype='datetime64[ns]'))

        self.create_response = {"TimeSeriesId":'tsfromhell'}

        self.client = timeseriesclient.TimeSeriesClient(auth)
        self.client._files_api.commit = Mock(return_value=200)
        self.client._timeseries_api.create = Mock(return_value=self.create_response)
        self.client._files_api.upload = Mock(return_value=self.dummy_params)
        self.client._verify_and_prepare_dataframe = Mock()
        self.client._get_reference_time = Mock(return_value='1970-01-01T00:00:00')
        self.client._get_file_status = Mock(side_effect=["Processing", "Processing", "Ready"])

    @patch('timeseriesclient.rest_api.FilesApi.upload_service')
    def test_all_calls_made(self, mock_upload_service):
        mock_upload_service.return_value = timeseriesclient.rest_api.files_api.AzureBlobService
        df = pd.DataFrame({'a':np.arange(1e3)})
        self.client.create(df)

        self.client._files_api.upload.assert_called_with(self.dummy_token)
        mock_upload_service.assert_called_with(self.dummy_params)
        self.client._files_api.commit.assert_called_with(self.dummy_token, self.dummy_params['FileId'])
        self.assertTrue(self.client._timeseries_api.create.called)

    @patch('timeseriesclient.rest_api.FilesApi.upload_service')
    def test_create_file_endpoint_called(self, mock_upload_service):
        mock_upload_service.return_value = timeseriesclient.rest_api.files_api.AzureBlobService
        result = self.client.create(self.dummy_df)
        exp_id = 666
        self.client._timeseries_api.create.assert_called_once_with(self.dummy_token,
                                                              exp_id)
        self.assertEqual(result, {"TimeSeriesId":'tsfromhell'})


class Test_Append(unittest.TestCase):

    def setUp(self):
        self.dummy_token = {'accessToken' : 'abcdef',
                      'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's')}
        auth = Mock()
        auth.token = self.dummy_token

        self.client = timeseriesclient.TimeSeriesClient(auth)

        self.file_id = 'file123abc'

        self.client._verify_and_prepare_dataframe = Mock()
        self.client._upload_file = Mock(return_value=self.file_id)
        self.client._wait_until_file_ready = Mock(side_effect=["Processing", "Processing", "Ready"])

        self.dummy_df = pd.DataFrame({'a':np.arange(10)}, index=np.array(np.arange(10), dtype='datetime64[ns]'))
        self.client._timeseries_api = Mock()



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
        self.client = timeseriesclient.TimeSeriesClient(Mock())

    def test_datetime64(self):
        df = pd.DataFrame({'values' : np.random.rand(10)},
                          index=np.arange(0, 10e9, 1e9).astype('datetime64[ns]'))
        result = self.client._verify_and_prepare_dataframe(df)
        self.assertIsNone(result)

    def test_int64(self):
        df = pd.DataFrame({'values' : np.random.rand(10)},
                          index=np.arange(0, 10e9, 1e9).astype('int64'))
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

    def test_list(self):
        self.dummy_token ={'accessToken' : 'dummyToken' ,
                           'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
    
        auth = Mock()
        auth.token = self.dummy_token
        client = timeseriesclient.TimeSeriesClient(auth)

        test_list = ['ts1', 'ts2', 'ts3']
        client._timeseries_api = Mock()
        client._timeseries_api.list.return_value = test_list

        timeseries = client.list()

        self.assertEqual(timeseries, test_list)


class Test_Info(unittest.TestCase):
    def test_info(self):
        self.dummy_token ={'accessToken' : 'dummyToken' ,
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }

        auth = Mock()
        auth.token = self.dummy_token
        client = timeseriesclient.TimeSeriesClient(auth)

        client._timeseries_api = Mock()
        client._timeseries_api.info.return_value = {'TimeSeriesId': 'someId'}

        result = client.info('someId')

        client._timeseries_api.info.assert_called_with(self.dummy_token, 'someId')
        self.assertIsInstance(result, dict)
        self.assertEqual(result["TimeSeriesId"], "someId") 


class Test_DeleteTimeSeries(unittest.TestCase):

    def test_delete(self):
        self.dummy_token ={'accessToken' : 'dummyToken' ,
                           'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
    
        auth = Mock()
        auth.token = self.dummy_token
        client = timeseriesclient.TimeSeriesClient(auth)
    
        client._timeseries_api = Mock()

        response = client.delete('123456')

        client._timeseries_api.delete.assert_called_with(self.dummy_token, '123456')


class Test_Get(unittest.TestCase):

    def setUp(self):
        self.dummy_token = {'accessToken' : 'abcdef',
                            'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        auth = Mock()
        auth.token = self.dummy_token

        self.client = timeseriesclient.TimeSeriesClient(auth)

        self.client._timeseries_api = Mock()

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


if __name__ == '__main__':
    unittest.main()
