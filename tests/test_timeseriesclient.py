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


timeseriesclient.globalsettings.environment.set_qa()  # Test should not make calls to the API, but just in case!


class Test_TimeSeriesClient(unittest.TestCase):

    def setUp(self):
        self.auth = Mock()
        self.auth.token = {'accessToken': 'abcdef',
                           'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}

        self.client = timeseriesclient.TimeSeriesClient(self.auth)
        self.client._files_api = Mock()
        self.client._timeseries_api = Mock()

        self.dummy_df = pd.DataFrame({'a':np.arange(1e3)}, index=np.array(np.arange(1e3), dtype='datetime64[ns]'))

        self.timeseries_id = 'abc-123-xyz'
        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }

        self.response = Mock()
        self.response.text = '1,1\n2,2\n3,3\n4,4'
        self.response_df = pd.DataFrame(data={'values': [1, 2, 3, 4]}, index=[1, 2, 3, 4])
        self.response_df.index.name = 'time'

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

    def test_append_all_methods_called(self):
        self.client._verify_and_prepare_dataframe = Mock(return_value=None)
        self.client._upload_file = Mock(return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.add.return_value = expected_response

        response = self.client.append(self.dummy_df, self.timeseries_id)

        self.client._verify_and_prepare_dataframe.assert_called_once_with(self.dummy_df)
        self.client._upload_file.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(self.dummy_params['FileId'])
        self.client._timeseries_api.add.assert_called_once_with(self.auth.token, self.timeseries_id, self.dummy_params['FileId'])
        self.assertDictEqual(response, expected_response)

    def test_list_all_methods_called(self):
        expected_response = ['ts1', 'ts2', 'ts3']
        self.client._timeseries_api.list.return_value = expected_response

        response = self.client.list()

        self.client._timeseries_api.list.assert_called_once_with(self.auth.token)
        self.assertListEqual(response, expected_response)

    def test_info_all_methods_called(self):
        expected_response = {'abc': 123}
        self.client._timeseries_api.info.return_value = expected_response

        response = self.client.info(self.timeseries_id)

        self.client._timeseries_api.info.assert_called_once_with(self.auth.token, self.timeseries_id)
        self.assertDictEqual(response, expected_response)

    def test_delete_all_methods_called(self):
        expected_response = 200
        self.client._timeseries_api.delete.return_value = expected_response

        response = self.client.delete(self.timeseries_id)

        self.client._timeseries_api.delete.assert_called_once_with(self.auth.token, self.timeseries_id)
        self.assertEqual(response, expected_response)

    def test_get_all_methods_called_with_default(self):
        self.client._timeseries_api.data.return_value = self.response

        response = self.client.get(self.timeseries_id)

        self.client._timeseries_api.data.assert_called_once_with(self.auth.token, self.timeseries_id, timeseriesclient.timeseriesclient._START_DEFAULT, timeseriesclient.timeseriesclient._END_DEFAULT)

        pd.util.testing.assert_series_equal(response, self.response_df['values'])

    def test_get_all_methods_called_with_default(self):
        self.client._timeseries_api.data.return_value = self.response

        response = self.client.get(self.timeseries_id)

        self.client._timeseries_api.data.assert_called_once_with(self.auth.token, self.timeseries_id, timeseriesclient.timeseriesclient._START_DEFAULT, timeseriesclient.timeseriesclient._END_DEFAULT)

        pd.util.testing.assert_series_equal(response, self.response_df['values'])

    def test_get_all_methods_called_with_convert_datetime(self):
        self.client._timeseries_api.data.return_value = self.response

        response = self.client.get(self.timeseries_id, convert_date=True)

        self.client._timeseries_api.data.assert_called_once_with(self.auth.token, self.timeseries_id, timeseriesclient.timeseriesclient._START_DEFAULT, timeseriesclient.timeseriesclient._END_DEFAULT)

        df = self.response_df
        df.index = pd.to_datetime(df.index)
        pd.util.testing.assert_series_equal(response, df['values'])

    def test_get_all_methods_called_with_start_stop_as_str(self):
        self.client._timeseries_api.data.return_value = self.response

        response = self.client.get(self.timeseries_id,
                                   start='1970-01-01 00:00:00.000000001',
                                   end='1970-01-01 00:00:00.000000004')

        self.client._timeseries_api.data.assert_called_once_with(self.auth.token, self.timeseries_id, 1, 4)

        pd.util.testing.assert_series_equal(response, self.response_df['values'])

    def test_get_start_stop_exception(self):
        self.client._timeseries_api.data.return_value = self.response

        with self.assertRaises(ValueError):
            response = self.client.get(self.timeseries_id,
                                       start='1970-01-01 00:00:00.000000004',
                                       end='1970-01-01 00:00:00.000000001')


class Test_TimeSeriesClient_verify_prep_dataframe(unittest.TestCase):
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

    def test_index_not_valid(self):
        df = pd.DataFrame({'values' : np.random.rand(10)},
                          index=np.arange(0, 10e9, 1e9).astype('float'))
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_dataframe(df)

    def test_not_a_dataframe(self):
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_dataframe('this is wrong input')

    def test_too_many_columns(self):
        df = pd.DataFrame({'values' : np.random.rand(10)},
                          index=np.arange(0, 10e9, 1e9).astype('datetime64[ns]'))
        df['values2'] = df['values'].values

        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_dataframe(df)


class Test_TimeSeriesClient_private_funcs(unittest.TestCase):
    def setUp(self):
        self.auth = Mock()
        self.auth.token = {'accessToken': 'abcdef',
                           'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}

        self.dummy_params = { 'FileId' : 666,
                              'Account' : 'account',
                              'SasKey' : 'abcdef',
                              'Container' : 'blobcontainer', 
                              'Path' : 'blobpath',
                              'Endpoint' : 'endpointURI' }

        self.client = timeseriesclient.TimeSeriesClient(self.auth)
        self.client._files_api = Mock()

        self.dummy_df = pd.DataFrame({'a': np.arange(1e3)},
                                     index=np.array(np.arange(1e3), dtype='datetime64[ns]'))

        self.uploader = Mock()
        self.uploader.file_id = 123
        self.client._files_api.upload_service.return_value = self.uploader

    def test__upload_file(self):
        upload_params = {'parmas': 123}
        self.client._files_api.upload.return_value = upload_params

        response = self.client._upload_file(self.dummy_df)

        self.client._files_api.upload.assert_called_once_with(self.auth.token)
        self.client._files_api.upload_service.assert_called_once_with(upload_params)
        self.uploader.create_blob_from_df.assert_called_once_with(self.dummy_df)
        self.client._files_api.commit.assert_called_once_with(self.auth.token,
                                                              self.uploader.file_id)

        self.assertEqual(response, self.uploader.file_id)

    def test__get_file_status(self):
        response = {'State': 'Ready'}
        self.client._files_api.status.return_value = response

        status = self.client._get_file_status(self.dummy_params['FileId'])

        self.client._files_api.status.assert_called_once_with(self.auth.token,
                                                              self.dummy_params['FileId'])

        self.assertEqual(status, response['State'])

    @patch('time.sleep')
    def test__wait_until_file_ready_ready(self, mock_sleep):
        self.client._get_file_status = Mock(side_effect=['Processing', 'Ready'])
        status = self.client._wait_until_file_ready(self.dummy_params['FileId'])

        self.client._get_file_status.assert_called_with(self.dummy_params['FileId'])
        self.assertEqual(status, 'Ready')

    def test__wait_until_file_ready_fail(self):
        self.client._get_file_status = Mock(return_value='Failed')
        status = self.client._wait_until_file_ready(self.dummy_params['FileId'])

        self.client._get_file_status.assert_called_with(self.dummy_params['FileId'])
        self.assertEqual(status, 'Failed')


if __name__ == '__main__':
    unittest.main()
