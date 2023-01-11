import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import requests

import datareservoirio
from datareservoirio import Client


# Test should not make calls to the API, but just in case!
def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_Client(unittest.TestCase):
    @patch("datareservoirio.client.FileCacheDownload")
    def setUp(self, mock_cache):
        self.auth = Mock()

        self.client = Client(self.auth)
        self.client._files_api = Mock()
        self.client._timeseries_api = Mock()
        self.client._metadata_api = Mock()
        self._storage = self.client._storage = Mock()

        self.dummy_df = pd.Series(
            np.arange(1e3), index=np.array(np.arange(1e3), dtype="datetime64[ns]")
        )

        self.timeseries_id = "abc-123-xyz"
        self.dummy_params = {
            "FileId": 666,
            "Account": "account",
            "SasKey": "abcdef",
            "Container": "blobcontainer",
            "Path": "blobpath",
            "Endpoint": "endpointURI",
            "Files": [],
        }

        self.response = Mock()
        self.response.text = "1,1\n2,2\n3,3\n4,4"

        self.series_with_10_rows = pd.Series(
            data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )
        self.series_with_10_rows.index = pd.to_datetime(
            self.series_with_10_rows.index, utc=True
        )

        self.series_with_10_rows_csv = self.series_with_10_rows.to_csv(header=False)

        self.download_days_response = {
            "Files": [
                {
                    "Index": 0,
                    "FileId": "00000000-3ad3-4b13-b452-d2c212fab6f1",
                    "Chunks": [
                        {
                            "Account": "reservoirfiles00test",
                            "SasKey": "sv=2016-05-31&sr=b&sig=HF58vgk5RTKB8pN6SXp40Ih%2FRhsHnyJPh8fTqzbVcKM%3D&se=2017-05-22T15%3A25%3A59Z&sp=r",
                            "SasKeyExpirationTime": "2022-08-25T06:59:06.591Z",
                            "Container": "timeseries-days",
                            "Path": "5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/17/16238.csv",
                            "Endpoint": "https://timeseries-days/5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/17/16238.csv",
                        }
                    ],
                },
                {
                    "Index": 1,
                    "FileId": "10000000-3ad3-4b13-b452-d2c212fab6f1",
                    "Chunks": [
                        {
                            "Account": "reservoirfiles00test",
                            "SasKey": "sv=2016-05-31&sr=b&sig=9u%2Fg5BY%2BODexgRvV0Bt6OMoM6Wr5zCyDL7vRP%2B2zrtc%3D&se=2017-05-22T15%3A25%3A59Z&sp=r",
                            "SasKeyExpirationTime": "2022-08-25T06:59:06.591Z",
                            "Container": "timeseries-days",
                            "Path": "5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv",
                            "Endpoint": "https://timeseries-days/5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv",
                        }
                    ],
                },
                {
                    "Index": 2,
                    "FileId": "20000000-3ad3-4b13-b452-d2c212fab6f1",
                    "Chunks": [
                        {
                            "Account": "reservoirfiles00test",
                            "SasKey": "sv=2016-05-31&sr=b&sig=9u%2Fg5BY%2BODexgRvV0Bt6OMoM6Wr5zCyDL7vRP%2B2zrtc%3D&se=2017-05-22T15%3A25%3A59Z&sp=r",
                            "SasKeyExpirationTime": "2022-08-25T06:59:06.591Z",
                            "Container": "timeseries-days",
                            "Path": "5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv",
                            "Endpoint": "https://timeseries-days/5a5fc7b8-3ad3-4b13-b452-d2c212fab6f1/2014/06/15/16236.csv",
                        }
                    ],
                },
            ]
        }

    def test_init(self):
        self.assertIsInstance(self.client, datareservoirio.Client)
        self.assertIsInstance(self.client._timeseries_api, Mock)
        self.assertIsInstance(self.client._metadata_api, Mock)
        self.assertIsInstance(self.client._files_api, Mock)
        self.assertIsInstance(self.client._storage, Mock)

    @patch("datareservoirio.client.DirectDownload")
    def test_init_with_cache_disabled(self, mock_dl):
        with Client(self.auth, cache=False):
            assert mock_dl.call_count == 1

    @patch("datareservoirio.client.FileCacheDownload")
    def test_init_with_defaults_cache_is_enabled_and_format_parquet(self, mock_cache):
        with Client(self.auth):
            kwargs = mock_cache.call_args[1]
            self.assertIn("format_", kwargs)
            self.assertEqual(kwargs["format_"], "parquet")
            cache_defaults = Client.CACHE_DEFAULT.copy()
            cache_defaults["format_"] = cache_defaults.pop("format")
            mock_cache.assert_called_once_with(**cache_defaults)

    @patch("datareservoirio.client.FileCacheDownload")
    def test_init_with_cache_enabled(self, mock_cache):
        with Client(self.auth, cache=True):
            cache_defaults = Client.CACHE_DEFAULT.copy()
            cache_defaults["format_"] = cache_defaults.pop("format")
            mock_cache.assert_called_once_with(**cache_defaults)

    @patch("datareservoirio.client.FileCacheDownload")
    def test_init_with_cache_format_csv(self, mock_cache):
        with Client(self.auth, cache=True, cache_opt={"format": "csv"}):
            kwargs = mock_cache.call_args[1]
            self.assertIn("format_", kwargs)
            self.assertEqual(kwargs["format_"], "csv")

    @patch("datareservoirio.client.FileCacheDownload")
    def test_init_with_cache_format_parquet(self, mock_cache):
        with Client(self.auth, cache={"format": "parquet"}):
            kwargs = mock_cache.call_args[1]
            self.assertIn("format_", kwargs)
            self.assertEqual(kwargs["format_"], "parquet")

    def test_init_with_invalid_cache_format_raises_exception(self):
        with self.assertRaises(ValueError):
            with Client(self.auth, cache=True, cache_opt={"format": "bogusformat"}):
                pass

    @patch("datareservoirio.client.FileCacheDownload")
    def test_init_with_cache_root(self, mock_cache):
        cache_defaults = Client.CACHE_DEFAULT.copy()
        cache_defaults["format_"] = cache_defaults.pop("format")
        cache_defaults["cache_root"] = "a:\\diskett"

        with Client(self.auth, cache=True, cache_opt={"cache_root": "a:\\diskett"}):
            mock_cache.assert_called_once_with(**cache_defaults)

    @patch("datareservoirio.client.FileCacheDownload")
    def test_init_with_cache_max_size(self, mock_cache):
        cache_defaults = Client.CACHE_DEFAULT.copy()
        cache_defaults["format_"] = cache_defaults.pop("format")
        cache_defaults["max_size"] = 10

        with Client(self.auth, cache=True, cache_opt={"max_size": 10}):
            mock_cache.assert_called_once_with(**cache_defaults)

    def test_ping_request(self):
        self.client._files_api.ping.return_value = {"status": "pong"}

        response = self.client.ping()
        self.assertEqual(response, {"status": "pong"})

    @patch("time.sleep")
    def test_create_without_data(self, mock_sleep):
        expected_response = {"abc": 123}
        self.client._timeseries_api.create.return_value = expected_response

        response = self.client.create()

        self.client._timeseries_api.create.assert_called_once_with()
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_create_with_data(self, mock_sleep):
        self._storage.put = Mock(return_value=self.dummy_params["FileId"])
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.create_with_data.return_value = expected_response

        response = self.client.create(self.dummy_df)

        self._storage.put.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params["FileId"]
        )
        self.client._timeseries_api.create_with_data.assert_called_once_with(
            self.dummy_params["FileId"]
        )
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_create_with_data_without_wait_on_verification(self, mock_sleep):
        self._storage.put = Mock(return_value=self.dummy_params["FileId"])
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.create_with_data.return_value = expected_response

        response = self.client.create(self.dummy_df, wait_on_verification=False)

        self._storage.put.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_not_called()

        self.client._timeseries_api.create_with_data.assert_called_once_with(
            self.dummy_params["FileId"]
        )
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_create_when_timeseries_have_duplicate_indicies_throws(self, mock_sleep):
        self._storage.put = Mock(return_value=self.dummy_params["FileId"])
        self.client._wait_until_file_ready = Mock(return_value="Ready")
        df = pd.Series([0.0, 1.0, 2.0, 3.1, 3.2, 3.3, 4.0], index=[0, 1, 2, 3, 3, 3, 4])

        with self.assertRaises(ValueError):
            self.client.create(df)

    @patch("time.sleep")
    def test_append_all_methods_called(self, mock_sleep):
        self.client._verify_and_prepare_series = Mock(return_value=None)
        self._storage.put = Mock(return_value=self.dummy_params["FileId"])
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.add.return_value = expected_response

        response = self.client.append(self.dummy_df, self.timeseries_id)

        self.client._verify_and_prepare_series.assert_called_once_with(self.dummy_df)
        self._storage.put.assert_called_once_with(self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params["FileId"]
        )
        self.client._timeseries_api.add.assert_called_once_with(
            self.timeseries_id, self.dummy_params["FileId"]
        )
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_append_without_wait_on_verification(self, mock_sleep):
        self.client._verify_and_prepare_series = Mock(return_value=None)
        self._storage.put = Mock(return_value=self.dummy_params["FileId"])
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.add.return_value = expected_response

        response = self.client.append(
            self.dummy_df, self.timeseries_id, wait_on_verification=False
        )

        self.client._verify_and_prepare_series.assert_called_once_with(self.dummy_df)
        self._storage.put.assert_called_once_with(self.dummy_df)

        self.client._wait_until_file_ready.assert_not_called()

        self.client._timeseries_api.add.assert_called_once_with(
            self.timeseries_id, self.dummy_params["FileId"]
        )
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_append_when_timeseries_have_duplicate_indicies_throws(self, mock_sleep):
        self._storage.put = Mock(return_value=self.dummy_params["FileId"])
        self.client._wait_until_file_ready = Mock(return_value="Ready")
        df = pd.Series([0.0, 1.0, 2.0, 3.1, 3.2, 3.3, 4.0], index=[0, 1, 2, 3, 3, 3, 4])

        with self.assertRaises(ValueError):
            self.client.append(df, self.timeseries_id)

    def test_info_all_methods_called(self):
        expected_response = {"abc": 123}
        self.client._timeseries_api.info.return_value = expected_response

        response = self.client.info(self.timeseries_id)

        self.client._timeseries_api.info.assert_called_once_with(self.timeseries_id)
        self.assertDictEqual(response, expected_response)

    def test_delete_all_methods_called(self):
        expected_response = 200
        self.client._timeseries_api.delete.return_value = expected_response

        response = self.client.delete(self.timeseries_id)

        self.client._timeseries_api.delete.assert_called_once_with(self.timeseries_id)
        self.assertEqual(response, expected_response)

    def test_get_with_defaults(self):
        self._storage.get.return_value = self.series_with_10_rows

        response = self.client.get(self.timeseries_id)

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id,
            datareservoirio.client._START_DEFAULT,
            datareservoirio.client._END_DEFAULT - 1,
        )
        pd.testing.assert_series_equal(response, self.series_with_10_rows)

    def test_get_with_convert_date_returns_series(self):
        series_without_dt = pd.Series(
            data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )

        series_with_dt = pd.Series(
            data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )
        series_with_dt.index = pd.to_datetime(series_with_dt.index, utc=True)

        start = pd.to_datetime(1, dayfirst=True, unit="ns", utc=True).value
        end = pd.to_datetime(10, dayfirst=True, unit="ns", utc=True).value

        self.client._storage.get.return_value = series_without_dt
        response = self.client.get(self.timeseries_id, start, end, convert_date=True)

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id, start, end - 1
        )

        pd.testing.assert_series_equal(
            response, series_without_dt, check_index_type=True
        )

    def test_get_without_convert_date_returns_series(self):
        series_without_dt = pd.Series(
            data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )
        start = pd.to_datetime(1, dayfirst=True, unit="ns", utc=True).value
        end = pd.to_datetime(10, dayfirst=True, unit="ns", utc=True).value
        self.client._storage.get.return_value = series_without_dt

        response = self.client.get(self.timeseries_id, start, end, convert_date=False)

        self.client._storage.get.assert_called_once_with(
            self.timeseries_id, start, end - 1
        )
        pd.testing.assert_series_equal(
            response, series_without_dt, check_index_type=True
        )

    def test_get_with_start_stop_as_str_calls_storagewithnanonsinceepoch(self):
        self._storage.get.return_value = self.series_with_10_rows

        self.client.get(
            self.timeseries_id,
            start="1970-01-01 00:00:00.000000001",
            end="1970-01-01 00:00:00.000000004",
        )

        self.client._storage.get.assert_called_once_with(self.timeseries_id, 1, 3)

    def test_get_with_emptytimeseries_return_empty(self):
        self._storage.get.return_value = pd.Series(dtype="float64")
        response_expected = pd.Series(dtype="float64")
        response_expected.index = pd.to_datetime(response_expected.index, utc=True)

        response = self.client.get(
            self.timeseries_id,
            start="1970-01-01 00:00:00.000000001",
            end="1970-01-01 00:00:00.000000004",
            raise_empty=False,
        )

        pd.testing.assert_series_equal(response, response_expected, check_dtype=False)

    def test_get_with_raise_empty_throws(self):
        self._storage.get.return_value = pd.Series(dtype="float64")

        with self.assertRaises(ValueError):
            self.client.get(
                self.timeseries_id,
                start="1970-01-01 00:00:00.000000001",
                end="1970-01-01 00:00:00.000000004",
                raise_empty=True,
            )

    def test_get_start_stop_exception(self):
        self.client._timeseries_api.data.return_value = self.response

        with self.assertRaises(ValueError):
            self.client.get(
                self.timeseries_id,
                start="1970-01-01 00:00:00.000000004",
                end="1970-01-01 00:00:00.000000001",
            )

    def test_get_subtract_nanosecond(self):
        self._storage.get.return_value = self.series_with_10_rows

        self.client.get(
            self.timeseries_id,
            start="1970-01-01 00:00:00.000000001",
            end="1970-01-01 00:00:00.000000004",
        )

        self.client._storage.get.assert_called_once_with(self.timeseries_id, 1, 3)

    def test_search(self):
        self.client.search("test_namespace", "test_key", "test_name", 123)
        self.client._timeseries_api.search.assert_called_once_with(
            "test_namespace", "test_key", "test_name", 123
        )

    def test_metadata_get_with_id(self):
        self.client._metadata_api.get_by_id.return_value = {"Id": "123abc"}

        response = self.client.metadata_get(metadata_id="123rdrs")
        self.assertEqual(response, {"Id": "123abc"})

    def test_metadata_get_with_namespace_and_key(self):
        self.client._metadata_api.get.return_value = {"Id": "lookupbynsandkey"}

        response = self.client.metadata_get(namespace="ns", key="k")
        self.assertEqual(response, {"Id": "lookupbynsandkey"})

    def test_metadata_get_without_params_throws(self):
        with self.assertRaises(ValueError):
            self.client.metadata_get()

    def test_metadata_get_with_id_and_namespace_and_key_uses_id(self):
        self.client._metadata_api.get_by_id.return_value = {"Id": "lookupbyid"}

        response = self.client.metadata_get(
            metadata_id="123rdrs", namespace="ns", key="k"
        )
        self.assertEqual(response, {"Id": "lookupbyid"})

    def test_metadata_set(self):
        self.client._metadata_api.put.return_value = {"Id": "123abc"}

        response = self.client.metadata_set("hello", "world", test="ohyeah!")
        self.assertEqual(response, {"Id": "123abc"})

    def test_metadata_browse_namespace(self):
        self.client.metadata_browse()
        self.client._metadata_api.namespaces.assert_called_once_with()

    def test_metadata_browse_keys(self):
        self.client.metadata_browse(namespace="test_namespace")
        self.client._metadata_api.keys.assert_called_once_with("test_namespace")

    def test_metadata_browse_names(self):
        self.client._metadata_api.get.return_value = {"Value": "{}"}

        self.client.metadata_browse(namespace="test_namespace", key="test_key")
        self.client._metadata_api.get.assert_called_once_with(
            "test_namespace", "test_key"
        )

    def test_metadata_search_conjunctive_true(self):
        self.client.metadata_search(namespace="test_namespace", key="test_key")
        self.client._metadata_api.search.assert_called_once_with(
            "test_namespace", "test_key", True
        )

    def test_metadata_search_conjunctive_false(self):
        self.client.metadata_search(
            namespace="test_namespace", key="test_key", conjunctive=False
        )
        self.client._metadata_api.search.assert_called_once_with(
            "test_namespace", "test_key", False
        )

    def test_metadata_delete(self):
        self.client.metadata_delete("id123")
        self.client._metadata_api.delete.assert_called_once_with("id123")

    def test_set_metadata_with_namespace_and_key_creates_and_attaches(self):
        self.client._metadata_api.put.return_value = {"Id": "meta-id-2"}

        self.client.set_metadata(
            series_id="series-id-1", namespace="meta-ns-1", key="meta-key-1", Data=42
        )

        self.client._metadata_api.put.assert_called_once_with(
            "meta-ns-1", "meta-key-1", False, Data=42
        )
        self.client._timeseries_api.attach_metadata.assert_called_once_with(
            "series-id-1", ["meta-id-2"]
        )

    def test_set_metadata_with_overwrite_false_throws(self):
        response = requests.Response()
        response.status_code = 409
        self.client._metadata_api.put.side_effect = requests.exceptions.HTTPError(
            response=response
        )

        with self.assertRaises(ValueError):
            self.client.set_metadata(
                series_id="series-id-1",
                namespace="meta-ns-1",
                key="meta-key-1",
                Data=42,
            )

    def test_set_metadata_with_overwrite_true(self):
        self.client._metadata_api.put.return_value = {"Id": "meta-id-2"}

        self.client.set_metadata(
            series_id="series-id-1",
            namespace="meta-ns-1",
            key="meta-key-1",
            overwrite=True,
            Data=42,
        )

        self.client._metadata_api.put.assert_called_once_with(
            "meta-ns-1", "meta-key-1", True, Data=42
        )

    def test_set_metadata_with_metadataid_calls_attachmetadata_with_idsinarray(self):
        self.client.set_metadata(series_id="series-id-1", metadata_id="meta-id-2")
        self.client._timeseries_api.attach_metadata.assert_called_once_with(
            "series-id-1", ["meta-id-2"]
        )

    def test_remove_metadata(self):
        self.client.remove_metadata("series_123", "meta_abc")
        self.client._timeseries_api.detach_metadata.assert_called_once_with(
            "series_123", ["meta_abc"]
        )


class Test_TimeSeriesClient_verify_prep_series(unittest.TestCase):
    def setUp(self):
        self.client = Client(Mock())

    def test_datetime64(self):
        series = pd.Series(
            np.random.rand(10), index=np.arange(0, 10e9, 1e9).astype("datetime64[ns]")
        )
        result = self.client._verify_and_prepare_series(series)
        self.assertIsNone(result)

    def test_int64(self):
        series = pd.Series(
            np.random.rand(10), index=np.arange(0, 10e9, 1e9).astype("int64")
        )
        result = self.client._verify_and_prepare_series(series)
        self.assertIsNone(result)

    def test_index_not_valid(self):
        series = pd.Series(
            np.random.rand(10), index=np.arange(0, 10e9, 1e9).astype("float")
        )
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_series(series)

    def test_not_a_series(self):
        with self.assertRaises(ValueError):
            self.client._verify_and_prepare_series("this is wrong input")


if __name__ == "__main__":
    unittest.main()
