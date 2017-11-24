import json
import unittest

import numpy as np
import pandas as pd
import requests

import datareservoirio
import datareservoirio.globalsettings as gs
from datareservoirio import Authenticator
from datareservoirio.rest_api import TimeSeriesAPI

try:
    from unittest.mock import Mock, MagicMock, patch, PropertyMock
except:
    from mock import Mock, MagicMock, patch, PropertyMock


# Test should not make calls to the API, but just in case!
def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_Client(unittest.TestCase):

    def setUp(self):
        self.auth = Mock()
        self.auth.token = {'accessToken': 'abcdef',
                           'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}

        self.client = datareservoirio.Client(self.auth)
        self.client._files_api = Mock()
        self.client._timeseries_api = Mock()
        self.downloader = Mock()
        self.client._files_api.transfer_service.return_value = self.downloader

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

    def test_token(self):
        self.assertEqual(self.client.token, self.auth.token)

    def test_ping_request(self):
        self.client._files_api.ping.return_value = {'status': 'pong'}

        response = self.client.ping()
        self.assertEqual(response, {'status': 'pong'})

    def test_create_all_methods_called(self):
        self.client._verify_and_prepare_series = Mock(return_value=None)
        self.client._upload_series = Mock(
            return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.create.return_value = expected_response

        response = self.client.create(self.dummy_df)

        self.client._verify_and_prepare_series.assert_called_once_with(
            self.dummy_df)
        self.client._upload_series.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params['FileId'])
        self.client._timeseries_api.create.assert_called_once_with(
            self.auth.token, self.dummy_params['FileId'])
        self.assertDictEqual(response, expected_response)

    def test_append_all_methods_called(self):
        self.client._verify_and_prepare_series = Mock(return_value=None)
        self.client._upload_series = Mock(
            return_value=self.dummy_params['FileId'])
        self.client._wait_until_file_ready = Mock(return_value='Ready')

        expected_response = {'abc': 123}
        self.client._timeseries_api.add.return_value = expected_response

        response = self.client.append(self.dummy_df, self.timeseries_id)

        self.client._verify_and_prepare_series.assert_called_once_with(
            self.dummy_df)
        self.client._upload_series.assert_called_once_with(self.dummy_df)
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

    def test_get_all_methods_called_with_default(self):
        self.client._timeseries_api.download_days.return_value = self.download_days_response
        self.downloader.get_blob_to_series.return_value = self.dataframe_with_10_rows
        self.client._download_series = Mock(return_value=self.dataframe_with_10_rows)

        response = self.client.get(self.timeseries_id)

        self.client._timeseries_api.download_days.assert_called_once_with(
            self.auth.token, self.timeseries_id, datareservoirio.client._START_DEFAULT, datareservoirio.client._END_DEFAULT)
        self.client._download_series.assert_called_once_with(self.download_days_response)
        pd.util.testing.assert_series_equal(
            response, self.dataframe_with_10_rows['values'])

    def test_get_all_methods_called_with_overflow(self):
        self.client._timeseries_api.download_days.return_value = self.download_days_response
        self.downloader.get_blob_to_series.return_value = self.dataframe_with_10_rows
        self.client._download_series = Mock(return_value=self.dataframe_with_10_rows)

        response = self.client.get(self.timeseries_id, start=2, end=3)

        self.client._timeseries_api.download_days.assert_called_once_with(
            self.auth.token, self.timeseries_id, 2, 3)
        self.client._download_series.assert_called_once_with(self.download_days_response)
        pd.util.testing.assert_series_equal(
            response, self.dataframe_with_10_rows['values'].loc[2:3])

    def test_get_all_methods_called_with_convert_datetime(self):
        start = pd.to_datetime(1, dayfirst=True, unit='ns').value
        end = pd.to_datetime(10, dayfirst=True, unit='ns').value
        self.client._timeseries_api.download_days.return_value = self.download_days_response
        self.downloader.get_blob_to_series.return_value = self.dataframe_with_10_rows
        self.client._download_series = Mock(return_value=self.dataframe_with_10_rows)

        response = self.client.get(
            self.timeseries_id, start, end, convert_date=True)

        self.client._timeseries_api.download_days.assert_called_once_with(
            self.auth.token, self.timeseries_id, start, end)
        self.client._download_series.assert_called_once_with(self.download_days_response)
        self.dataframe_with_10_rows.index = pd.to_datetime(
            self.dataframe_with_10_rows.index)
        pd.util.testing.assert_series_equal(
            response, self.dataframe_with_10_rows['values'])

    def test_get_all_methods_called_with_start_stop_as_str(self):
        self.client._timeseries_api.download_days.return_value = self.download_days_response
        self.downloader.get_blob_to_series.return_value = self.dataframe_with_10_rows

        response = self.client.get(self.timeseries_id,
                                   start='1970-01-01 00:00:00.000000001',
                                   end='1970-01-01 00:00:00.000000004')

        self.client._timeseries_api.download_days.assert_called_once_with(
            self.auth.token, self.timeseries_id, 1, 4)
        pd.util.testing.assert_series_equal(
            response, self.dataframe_with_10_rows['values'].loc[1:4])

    def test_get_return_empty(self):
        self.client._timeseries_api.download_days.return_value = self.download_days_response
        self.downloader.get_blob_to_series.return_value = pd.DataFrame(columns=['index', 'values'])

        response = self.client.get(self.timeseries_id,
                                   start='1970-01-01 00:00:00.000000001',
                                   end='1970-01-01 00:00:00.000000004', raise_empty=False)

        response_expected = pd.Series(name='values')
        pd.testing.assert_series_equal(response, response_expected, check_dtype=False)  # fix in future. dtype should be checked

    def test_get_raise_empty(self):
        self.client._timeseries_api.download_days.return_value = self.download_days_response
        self.downloader.get_blob_to_series.return_value = pd.DataFrame(columns=['index', 'values'])

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
        self.client = datareservoirio.Client(Mock())

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


class Test_TimeSeriesClient_private_funcs(unittest.TestCase):

    def setUp(self):
        self.auth = Mock()
        self.auth.token = {'accessToken': 'abcdef',
                           'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}

        self.dummy_params = {'FileId': 666,
                             'Account': 'account',
                             'SasKey': 'abcdef',
                             'Container': 'blobcontainer',
                             'Path': 'blobpath',
                             'Endpoint': 'endpointURI',
                             'Files': [{'Chunks': ['dummy_chunk_1', 'dummy_chunk_2']},
                                       {'Chunks': ['dummy_chunk_1', 'dummy_chunk_2']}]}

        self.client = datareservoirio.Client(self.auth)
        self.client._files_api = Mock()

        self.dummy_df = pd.DataFrame({'a': np.arange(1e3)},
                                     index=np.array(np.arange(1e3), dtype='datetime64[ns]'))

        self.uploader = Mock()
        self.client._files_api.transfer_service.return_value = self.uploader

    def test__upload_file(self):
        upload_params = {'parmas': 123, 'FileId': '123abc'}
        self.client._files_api.upload.return_value = upload_params

        response = self.client._upload_series(self.dummy_df)

        self.client._files_api.upload.assert_called_once_with(self.auth.token)
        self.client._files_api.transfer_service.assert_called_once_with(
            upload_params)
        self.uploader.create_blob_from_series.assert_called_once_with(
            self.dummy_df)
        self.client._files_api.commit.assert_called_once_with(self.auth.token,
                                                              upload_params['FileId'])

        self.assertEqual(response, upload_params['FileId'])

    def test__get_file_status(self):
        response = {'State': 'Ready'}
        self.client._files_api.status.return_value = response

        status = self.client._get_file_status(self.dummy_params['FileId'])

        self.client._files_api.status.assert_called_once_with(self.auth.token,
                                                              self.dummy_params['FileId'])

        self.assertEqual(status, response['State'])

    @patch('time.sleep')
    def test__wait_until_file_ready_ready(self, mock_sleep):
        self.client._get_file_status = Mock(
            side_effect=['Processing', 'Ready'])
        status = self.client._wait_until_file_ready(
            self.dummy_params['FileId'])

        self.client._get_file_status.assert_called_with(
            self.dummy_params['FileId'])
        self.assertEqual(status, 'Ready')

    def test__wait_until_file_ready_fail(self):
        self.client._get_file_status = Mock(return_value='Failed')
        status = self.client._wait_until_file_ready(
            self.dummy_params['FileId'])

        self.client._get_file_status.assert_called_with(
            self.dummy_params['FileId'])
        self.assertEqual(status, 'Failed')

    def test__download_chunks_as_dataframe_empty_chunks(self):
        df_returned = self.client._download_chunks_as_dataframe([])
        df_expected = pd.DataFrame(columns=['index', 'values'])
        pd.testing.assert_frame_equal(df_expected, df_returned)

    def test__download_chunks_as_dataframe_w_chunks(self):
        self.uploader.get_blob_to_series = Mock(side_effect=[
            pd.DataFrame([0, 1, 2], index=[0, 1, 2], columns=['values']),
            pd.DataFrame([3, 4, 5], index=[3, 4, 5], columns=['values'])])

        df_returned = self.client._download_chunks_as_dataframe(['dummy_chunk_1', 'dummy_chunk_2'])
        df_expected = pd.DataFrame([0, 1, 2, 3, 4, 5], index=[0, 1, 2, 3, 4, 5], columns=['values'])

        pd.testing.assert_frame_equal(df_expected, df_returned)

    @patch('datareservoirio.Client._download_chunks_as_dataframe')
    def test__download_series(self, mock_chunks):

        mock_chunks.side_effect = [
            pd.DataFrame([0., 1., 2.], index=[0, 1, 2], columns=['values']),
            pd.DataFrame([2., 3., 4.], index=[2, 3, 4], columns=['values'])]
        df_returned = self.client._download_series(self.dummy_params)

        df_expected = pd.DataFrame([0., 1., 2., 3., 4.], index=[0, 1, 2, 3, 4], columns=['values'])
        pd.testing.assert_frame_equal(df_expected, df_returned)

    @patch('datareservoirio.Client._download_chunks_as_dataframe')
    def test__download_series_with_unordered_series(self, mock_chunks):

        mock_chunks.side_effect = [
            pd.DataFrame([2., 3., 5.], index=[2, 3, 1], columns=['values']),
            pd.DataFrame([10., 11., 14., 16.], index=[0, 5, 4, 6], columns=['values'])]
        df_returned = self.client._download_series(self.dummy_params)

        df_expected = pd.DataFrame([10., 5., 2., 3., 14., 11., 16.], index=[0, 1, 2, 3, 4, 5, 6], columns=['values'])
        pd.testing.assert_frame_equal(df_expected, df_returned)

if __name__ == '__main__':
    unittest.main()
