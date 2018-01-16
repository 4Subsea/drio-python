import json
import unittest

import numpy as np
import pandas as pd
import requests

import datareservoirio
import datareservoirio.globalsettings as gs
from datareservoirio import Authenticator, Client
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import CachedDownloadStrategy, AlwaysDownloadStrategy, SimpleFileCache

try:
    from unittest.mock import Mock, MagicMock, patch, PropertyMock
except:
    from mock import Mock, MagicMock, patch, PropertyMock


# Test should not make calls to the API, but just in case!
def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_Client(unittest.TestCase):

    @patch('datareservoirio.client.SimpleFileCache')
    def setUp(self, mock_cache):
        self.auth = Mock()
        self.auth.token = {'accessToken': 'abcdef',
                           'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}

        self.client = Client(self.auth)
        self.client._files_api = Mock()
        self.client._timeseries_api = Mock()
        self._storage = self.client._storage = Mock()

        self.dummy_df = pd.DataFrame({'a': np.arange(1e3)}, index=np.array(
            np.arange(1e3), dtype='datetime64[ns]'))

        self.timeseries_id = 'abc-123-xyz'
        self.dummy_params = {'FileId': 666,
                             'Account': 'account',
                             'SasKey': 'abcdef',
                             'Container': 'blobcontainer',
                             'Path': 'blobpath',
                             'Endpoint': 'endpointURI',
                             'Files': []}

        self.response = Mock()
        self.response.text = u'1,1\n2,2\n3,3\n4,4'
        self.response_df = pd.DataFrame(data={'values': [1, 2, 3, 4]}, index=[
                                        1, 2, 3, 4], columns=['index', 'values'])
        self.dataframe_with_10_rows = pd.DataFrame(data={'values': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}, index=[
                                                   1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=['index', 'values'])
        self.dataframe_with_10_rows_csv = self.dataframe_with_10_rows.to_csv(
            header=False)
        self.response_df.index.name = 'index'
        self.download_days_response = {
            'Files':
                [
                    {'Index': 0,
                     'FileId': '00000000-3ad3-4b13-b452-d2c212fab6f1',
                     'Chunks': [
                        {
                            'Account': 'reservoirfiles00test',
                            'SasKey': 'sv=2016-05-31&sr=b&sig=HF58vgk5RTKB8pN6SXp40Ih%2FRhsHnyJPh8fTqzbVcKM%3D&se=2017-05-22T15%3A25%3A59Z&sp=r',
                            'Container': 'timeseries-days',
                            'Path': '5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/17/16238.csv',
                            'Endpoint': 'https://timeseries-days/5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/17/16238.csv'
                        }
                     ]
                     },
                    {'Index': 1,
                     'FileId': '10000000-3ad3-4b13-b452-d2c212fab6f1',
                     'Chunks': [
                        {
                            'Account': 'reservoirfiles00test',
                            'SasKey': 'sv=2016-05-31&sr=b&sig=9u%2Fg5BY%2BODexgRvV0Bt6OMoM6Wr5zCyDL7vRP%2B2zrtc%3D&se=2017-05-22T15%3A25%3A59Z&sp=r',
                            'Container': 'timeseries-days',
                            'Path': '5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv',
                            'Endpoint': 'https://timeseries-days/5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv'
                        }
                         ]},
                    {'Index': 2,
                     'FileId': '20000000-3ad3-4b13-b452-d2c212fab6f1',
                     'Chunks': [
                        {
                            'Account': 'reservoirfiles00test',
                            'SasKey': 'sv=2016-05-31&sr=b&sig=9u%2Fg5BY%2BODexgRvV0Bt6OMoM6Wr5zCyDL7vRP%2B2zrtc%3D&se=2017-05-22T15%3A25%3A59Z&sp=r',
                            'Container': 'timeseries-days',
                            'Path': '5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv',
                            'Endpoint': 'https://timeseries-days/5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv'
                        }
                         ]}
                ]}

    def test_init(self):
        self.assertIsInstance(self.client, datareservoirio.Client)
        self.assertIsInstance(self.client._authenticator, Mock)
        self.assertIsInstance(self.client._timeseries_api, Mock)
        self.assertIsInstance(self.client._files_api, Mock)
        self.assertIsInstance(self.client._storage, Mock)
    
    @patch('datareservoirio.client.AlwaysDownloadStrategy')
    def test_init_with_cache_disabled(self, mock_dl):
        client = Client(self.auth, cache={'enabled':False})
        mock_dl.assert_called_once_with()

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_defaults_cache_is_enabled_and_compressed(self, mock_cache):
        client = Client(self.auth)
        self.assertIsInstance(client._storage._downloader, CachedDownloadStrategy)
        mock_cache.assert_called_once_with(cache_root=None, compressionOn=True)

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_enabled(self, mock_cache):
        client = Client(self.auth, cache={'enabled':True})
        self.assertIsInstance(client._storage._downloader, CachedDownloadStrategy)
        mock_cache.assert_called_once_with(cache_root=None, compressionOn=True)

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_format_compressed_csv(self, mock_cache):
        client = Client(self.auth, cache={'format':'csv.gz'})
        self.assertIsInstance(client._storage._downloader, CachedDownloadStrategy)
        mock_cache.assert_called_once_with(cache_root=None, compressionOn=True)

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_format_uncompressed_csv(self, mock_cache):
        client = Client(self.auth, cache={'format':'csv'})
        self.assertIsInstance(client._storage._downloader, CachedDownloadStrategy)
        mock_cache.assert_called_once_with(cache_root=None, compressionOn=False)

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_invalid_cache_format_raises_exception(self, mock_cache):
        with self.assertRaises(ValueError):
            Client(self.auth, cache={'format':'bogusformat'})

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_root(self, mock_cache):
        client = Client(self.auth, cache={'cache_root':'a:\\diskett'})
        mock_cache.assert_called_once_with(cache_root='a:\\diskett', compressionOn=True)

    def test_token(self):
        self.assertEqual(self.client.token, self.auth.token)

    def test_ping_request(self):
        self.client._files_api.ping.return_value = {'status': 'pong'}

        response = self.client.ping()
        self.assertEqual(response, {'status': 'pong'})

    @patch('time.sleep')
    def test_create_all_methods_called(self, mock_sleep):
        self.client._verify_and_prepare_series = Mock(return_value=None)
        self._storage.put = Mock(
            return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.create.return_value = expected_response

        response = self.client.create(self.dummy_df)

        self.client._verify_and_prepare_series.assert_called_once_with(
            self.dummy_df)
        self._storage.put.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params['FileId'])
        self.client._timeseries_api.create.assert_called_once_with(
            self.auth.token, self.dummy_params['FileId'])
        self.assertDictEqual(response, expected_response)

    @patch('time.sleep')
    def test_append_all_methods_called(self, mock_sleep):
        self.client._verify_and_prepare_series = Mock(return_value=None)
        self._storage.put = Mock(
            return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.add.return_value = expected_response

        response = self.client.append(self.dummy_df, self.timeseries_id)

        self.client._verify_and_prepare_series.assert_called_once_with(
            self.dummy_df)
        self._storage.put.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params['FileId'])
        self.client._timeseries_api.add.assert_called_once_with(
            self.auth.token, self.timeseries_id, self.dummy_params['FileId'])
        self.assertDictEqual(response, expected_response)

    def test_list_all_methods_called(self):
        expected_response = ['ts1', 'ts2', 'ts3']
        self.client._timeseries_api.list.return_value = expected_response

        response = self.client.list()

        self.client._timeseries_api.list.assert_called_once_with(
            self.auth.token)
        self.assertListEqual(response, expected_response)

    def test_info_all_methods_called(self):
        expected_response = {'abc': 123}
        self.client._timeseries_api.info.return_value = expected_response

        response = self.client.info(self.timeseries_id)

        self.client._timeseries_api.info.assert_called_once_with(
            self.auth.token, self.timeseries_id)
        self.assertDictEqual(response, expected_response)

    def test_delete_all_methods_called(self):
        expected_response = 200
        self.client._timeseries_api.delete.return_value = expected_response

        response = self.client.delete(self.timeseries_id)

        self.client._timeseries_api.delete.assert_called_once_with(
            self.auth.token, self.timeseries_id)
        self.assertEqual(response, expected_response)

    def test_get_with_defaults(self):
        self._storage.get.return_value = self.dataframe_with_10_rows

        response = self.client.get(self.timeseries_id)

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id, datareservoirio.client._START_DEFAULT, datareservoirio.client._END_DEFAULT)
        pd.util.testing.assert_series_equal(
            response, self.dataframe_with_10_rows['values'])

    def test_get_with_convert_date_returns_dataframe(self):
        start = pd.to_datetime(1, dayfirst=True, unit='ns').value
        end = pd.to_datetime(10, dayfirst=True, unit='ns').value
        self.client._storage.get.return_value = self.dataframe_with_10_rows

        response = self.client.get(
            self.timeseries_id, start, end, convert_date=True)

        self.client._storage.get.assert_called_once_with(self.timeseries_id, start, end)
        pd.util.testing.assert_series_equal(
            response, self.dataframe_with_10_rows['values'])

    def test_get_with_start_stop_as_str_calls_storagewithnanonsinceepoch(self):
        self._storage.get.return_value = self.dataframe_with_10_rows

        response = self.client.get(self.timeseries_id,
                                   start='1970-01-01 00:00:00.000000001',
                                   end='1970-01-01 00:00:00.000000004')

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id, 1, 4)

    def test_get_with_emptytimeseries_return_empty(self):
        self._storage.get.return_value = pd.DataFrame(columns=['index', 'values'])

        response = self.client.get(self.timeseries_id,
                                   start='1970-01-01 00:00:00.000000001',
                                   end='1970-01-01 00:00:00.000000004', raise_empty=False)

        response_expected = pd.Series(name='values')
        pd.testing.assert_series_equal(response, response_expected, check_dtype=False)

    def test_get_with_raise_empty_throws(self):
        self._storage.get.return_value = pd.DataFrame(columns=['index', 'values'])

        with self.assertRaises(ValueError):
            self.client.get(self.timeseries_id, start='1970-01-01 00:00:00.000000001',
                            end='1970-01-01 00:00:00.000000004', raise_empty=True)

    def test_get_start_stop_exception(self):
        self.client._timeseries_api.data.return_value = self.response

        with self.assertRaises(ValueError):
            response = self.client.get(self.timeseries_id,
                                       start='1970-01-01 00:00:00.000000004',
                                       end='1970-01-01 00:00:00.000000001')


class Test_TimeSeriesClient_verify_prep_dataframe(unittest.TestCase):

    def setUp(self):
        self.client = Client(Mock())

    def test_datetime64(self):
        series = pd.Series(np.random.rand(10),
                           index=np.arange(0, 10e9, 1e9).astype('datetime64[ns]'))
        result = self.client._verify_and_prepare_series(series)
        self.assertIsNone(result)

    def test_int64(self):
        series = pd.Series(np.random.rand(10),
                           index=np.arange(0, 10e9, 1e9).astype('int64'))
        result = self.client._verify_and_prepare_series(series)
        self.assertIsNone(result)

    def test_index_not_valid(self):
        series = pd.Series(np.random.rand(10),
                           index=np.arange(0, 10e9, 1e9).astype('float'))
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_series(series)

    def test_not_a_series(self):
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_series('this is wrong input')

if __name__ == '__main__':
    unittest.main()
