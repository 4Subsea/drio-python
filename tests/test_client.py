import unittest
import numpy as np
import pandas as pd
import datareservoirio
from datareservoirio import Client

try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch


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
        self.client._metadata_api = Mock()
        self._storage = self.client._storage = Mock()

        self.dummy_df = pd.Series(np.arange(1e3), index=np.array(
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

        self.series_with_10_rows = pd.Series(data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                             index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        self.series_with_10_rows.index = pd.to_datetime(self.series_with_10_rows.index)

        self.series_with_10_rows_csv = self.series_with_10_rows.to_csv(header=False)

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
        self.assertIsInstance(self.client._metadata_api, Mock)
        self.assertIsInstance(self.client._files_api, Mock)
        self.assertIsInstance(self.client._storage, Mock)

    @patch('datareservoirio.client.AlwaysDownloadStrategy')
    def test_init_with_cache_disabled(self, mock_dl):
        with Client(self.auth, cache=False):
            assert mock_dl.call_count == 1

    @patch('datareservoirio.client.CachedDownloadStrategy')
    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_defaults_cache_is_enabled_and_format_msgpack(self, mock_cache, mock_dl):
        with Client(self.auth):
            kwargs = mock_dl.call_args[1]
            self.assertIn('format', kwargs)
            self.assertEqual(kwargs['format'], 'msgpack')
            cache_defalts = Client.CACHE_DEFAULT.copy()
            cache_defalts.pop('format')
            mock_cache.assert_called_once_with(**cache_defalts)

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_enabled(self, mock_cache):
        with Client(self.auth, cache=True):
            cache_defaults = Client.CACHE_DEFAULT.copy()
            cache_defaults.pop('format')
            mock_cache.assert_called_once_with(**cache_defaults)

    @patch('datareservoirio.client.CachedDownloadStrategy')
    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_format_csv(self, mock_cache, mock_dl):
        with Client(self.auth, cache=True, cache_opt={'format': 'csv'}):
            kwargs = mock_dl.call_args[1]
            self.assertIn('format', kwargs)
            self.assertEqual(kwargs['format'], 'csv')

    @patch('datareservoirio.client.CachedDownloadStrategy')
    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_format_msgpack(self, mock_cache, mock_dl):
        with Client(self.auth, cache={'format': 'msgpack'}):
            kwargs = mock_dl.call_args[1]
            self.assertIn('format', kwargs)
            self.assertEqual(kwargs['format'], 'msgpack')

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_invalid_cache_format_raises_exception(self, mock_cache):
        with self.assertRaises(ValueError):
            with Client(self.auth, cache=True, cache_opt={'format': 'bogusformat'}):
                pass

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_root(self, mock_cache):
        with Client(self.auth, cache=True, cache_opt={'cache_root': 'a:\\diskett'}):
            mock_cache.assert_called_once_with(cache_root='a:\\diskett', max_size=1024)

    @patch('datareservoirio.client.SimpleFileCache')
    def test_init_with_cache_max_size(self, mock_cache):
        with Client(self.auth, cache=True, cache_opt={'max_size': 10}):
            mock_cache.assert_called_once_with(cache_root=None, max_size=10)

    def test_token(self):
        self.assertEqual(self.client.token, self.auth.token)

    def test_ping_request(self):
        self.client._files_api.ping.return_value = {'status': 'pong'}

        response = self.client.ping()
        self.assertEqual(response, {'status': 'pong'})



    # def test_metadata(self):
    #     self.client._metadata_api.namespacekeys.return_value = ['pli', 'ihi']

    #     response = self.client.metadata()

    #     self.assertSequenceEqual(response, ['ihi', 'pli'])

    # def test_metadata_with_nskey(self):
    #     self.client._metadata_api.metadata.return_value = ['camp', 'anot']

    #     response = self.client.metadata('namesp', 'thekey')

    #     self.assertSequenceEqual(response, ['anot', 'camp'])

    # def test_timeseries_for_metadata(self):
    #     self.client._timeseries_api.timeseries_by_metadata.return_value = {'something':
    #                                                                        'thething',
    #                                                                        'else':
    #                                                                        'doesnt fly'}

    #     response = self.client.find_timeseries('ns', 'key', 'name')

    #     self.assertSequenceEqual(response, {'something': 'thething',
    #                                         'else': 'doesnt fly'})

    # def test_timeseries_for_metadata_value(self):
    #     self.client._timeseries_api.timeseries_by_metadatavalue.return_value = (
    #         {'1bf9d2b1-b544-4756-94b3-c60f67f8d112', '6ff4a077-06af-460a-82db-2a7fac53d443',
    #          '5c5bf184-941a-4a86-8154-9918a66d2e4f'}
    #     )

    #     response = self.client.find_timeseries('ns', 'key', 'name', 'value')

    #     self.assertSequenceEqual(response, {'1bf9d2b1-b544-4756-94b3-c60f67f8d112',
    #                                         '6ff4a077-06af-460a-82db-2a7fac53d443',
    #                                         '5c5bf184-941a-4a86-8154-9918a66d2e4f'})

    # def test_add_metadata(self):

    #     returnDict = {'Key': 'metatest', 'Created': '2018-06-04T09:04:57.420998+00:00',
    #                   'LastModified': None, 'CreatedByEmail': 'ihi@4subsea.com',
    #                   'Namespace': 'thefirst', 'Value': {'MetaName': 'The meta value'},
    #                   'LastModifiedByEmail': None, 'TimeSeries': [],
    #                   'Id': 'fbd96bf3-0cbd-41ec-7fbd-08d5c9fa3eba'}
    #     self.client._metadata_api.add_metadata.return_value = returnDict

    #     response = self.client.add_metadata('1bf9d2b1-b544-4756-94b3-c60f67f8d112',
    #                                         'some.thing', {'one': 'thing', 'another': 'thing'})

    #     self.assertDictEqual(response, returnDict)

    @patch('time.sleep')
    def test_create_without_data(self, mock_sleep):
        expected_response = {'abc': 123}
        self.client._timeseries_api.create.return_value = expected_response

        response = self.client.create()

        self.client._timeseries_api.create.assert_called_once_with(
            self.auth.token)
        self.assertDictEqual(response, expected_response)

    @patch('time.sleep')
    def test_create_with_data(self, mock_sleep):
        self._storage.put = Mock(
            return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.create_with_data.return_value = expected_response

        response = self.client.create(self.dummy_df)

        self._storage.put.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params['FileId'])
        self.client._timeseries_api.create_with_data.assert_called_once_with(
            self.auth.token, self.dummy_params['FileId'])
        self.assertDictEqual(response, expected_response)

    @patch('time.sleep')
    def test_create_when_timeseries_have_duplicate_indicies_throws(self, mock_sleep):
        self._storage.put = Mock(return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')
        df = pd.Series([0., 1., 2., 3.1, 3.2, 3.3, 4.], index=[0, 1, 2, 3, 3, 3, 4])

        with self.assertRaises(ValueError):
            self.client.create(df)

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

    @patch('time.sleep')
    def test_append_when_timeseries_have_duplicate_indicies_throws(self, mock_sleep):
        self._storage.put = Mock(return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')
        df = pd.Series([0., 1., 2., 3.1, 3.2, 3.3, 4.], index=[0, 1, 2, 3, 3, 3, 4])

        with self.assertRaises(ValueError):
            self.client.append(df, self.timeseries_id)

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
        self._storage.get.return_value = self.series_with_10_rows

        response = self.client.get(self.timeseries_id)

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id, datareservoirio.client._START_DEFAULT, datareservoirio.client._END_DEFAULT)
        pd.util.testing.assert_series_equal(
            response, self.series_with_10_rows)

    def test_get_with_convert_date_returns_series(self):
        series_without_dt = pd.Series(data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                      index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        series_with_dt = pd.Series(data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                   index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        series_with_dt.index = pd.to_datetime(series_with_dt.index)
        start = pd.to_datetime(1, dayfirst=True, unit='ns').value
        end = pd.to_datetime(10, dayfirst=True, unit='ns').value
        self.client._storage.get.return_value = series_without_dt

        response = self.client.get(
            self.timeseries_id, start, end, convert_date=True)

        self.client._storage.get.assert_called_once_with(self.timeseries_id, start, end)
        pd.util.testing.assert_series_equal(
            response, series_without_dt, check_index_type=True)

    def test_get_without_convert_date_returns_series(self):
        series_without_dt = pd.Series(data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                      index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        start = pd.to_datetime(1, dayfirst=True, unit='ns').value
        end = pd.to_datetime(10, dayfirst=True, unit='ns').value
        self.client._storage.get.return_value = series_without_dt

        response = self.client.get(
            self.timeseries_id, start, end, convert_date=False)

        self.client._storage.get.assert_called_once_with(self.timeseries_id, start, end)
        pd.util.testing.assert_series_equal(
            response, series_without_dt, check_index_type=True)

    def test_get_with_start_stop_as_str_calls_storagewithnanonsinceepoch(self):
        self._storage.get.return_value = self.series_with_10_rows

        self.client.get(self.timeseries_id,
                        start='1970-01-01 00:00:00.000000001',
                        end='1970-01-01 00:00:00.000000004')

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id, 1, 4)

    def test_get_with_emptytimeseries_return_empty(self):
        self._storage.get.return_value = pd.Series()
        response_expected = pd.Series()
        response_expected.index = pd.to_datetime(response_expected.index)

        response = self.client.get(self.timeseries_id,
                                   start='1970-01-01 00:00:00.000000001',
                                   end='1970-01-01 00:00:00.000000004', 
                                   raise_empty=False)

        pd.testing.assert_series_equal(response, response_expected, check_dtype=False)

    def test_get_with_raise_empty_throws(self):
        self._storage.get.return_value = pd.Series()

        with self.assertRaises(ValueError):
            self.client.get(self.timeseries_id,
                            start='1970-01-01 00:00:00.000000001',
                            end='1970-01-01 00:00:00.000000004',
                            raise_empty=True)

    def test_get_start_stop_exception(self):
        self.client._timeseries_api.data.return_value = self.response

        with self.assertRaises(ValueError):
            self.client.get(self.timeseries_id,
                            start='1970-01-01 00:00:00.000000004',
                            end='1970-01-01 00:00:00.000000001')

    def test_search(self):
        response = self.client.search('test_namespace', 'test_key', 'test_name', 123)
        self.client._timeseries_api.search.assert_called_once_with(
            self.client.token, 'test_namespace', 'test_key', 'test_name', 123)

    def test_metadata_create(self):
        self.client._metadata_api.create.return_value = {'Id': '123abc'}

        response = self.client.metadata_create('hello', 'world', test='ohyeah!')
        self.assertEqual(response, {'Id': '123abc'})

    def test_metadata_update(self):
        self.client._metadata_api.update.return_value = {'Id': '123abc'}

        response = self.client.metadata_update('123rdrs', 'hello', 'world', test='ohyeah!')
        self.assertEqual(response, {'Id': '123abc'})

    def test_metadata_get(self):
        self.client._metadata_api.get.return_value = {'Id': '123abc'}

        response = self.client.metadata_get('123rdrs')
        self.assertEqual(response, {'Id': '123abc'})

    def test_metadata_browse_namespace(self):
        response = self.client.metadata_browse()
        self.client._metadata_api.namespaces.assert_called_once_with(
            self.client.token)

    def test_metadata_browse_keys(self):
        response = self.client.metadata_browse(namespace='test_namespace')
        self.client._metadata_api.keys.assert_called_once_with(
            self.client.token, 'test_namespace')

    def test_metadata_browse_names(self):
        response = self.client.metadata_browse(namespace='test_namespace', key='test_key')
        self.client._metadata_api.names.assert_called_once_with(
            self.client.token, 'test_namespace', 'test_key')

    def test_metadata_search_conjunctive_true(self):
        response = self.client.metadata_search(namespace='test_namespace', key='test_key')
        self.client._metadata_api.search.assert_called_once_with(
            self.client.token, 'test_namespace', 'test_key', True)

    def test_metadata_search_conjunctive_false(self):
        response = self.client.metadata_search(
            namespace='test_namespace', key='test_key', conjunctive=False)
        self.client._metadata_api.search.assert_called_once_with(
            self.client.token, 'test_namespace', 'test_key', False)

    def test_set_metadata_with_metadataid_calls_attachmetadata_with_idsinarray(self):
        self.client.set_metadata(series_id='series-id-1', metadata_id='meta-id-2')
        self.client._timeseries_api.attach_metadata.assert_called_once_with(
            self.client.token, 'series-id-1', ['meta-id-2'])

    def test_remove_metadata(self):
        response = self.client.remove_metadata(
            'series_123', 'meta_abc')
        self.client._timeseries_api.detach_metadata.assert_called_once_with(
            self.client.token, 'series_123', ['meta_abc'])


class Test_TimeSeriesClient_verify_prep_series(unittest.TestCase):

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
