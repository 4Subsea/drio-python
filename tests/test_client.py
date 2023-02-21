import unittest
import warnings
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
import requests

import datareservoirio
from datareservoirio import Client


# Test should not make calls to the API, but just in case!
def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_Client(unittest.TestCase):
    @patch("datareservoirio.storage.StorageCache")
    def setUp(self, mock_cache):
        self.auth = Mock()

        self.client = Client(self.auth)
        self.client._files_api = Mock()
        self.client._timeseries_api = Mock()
        self.client._metadata_api = Mock()
        self._storage = self.client._storage = Mock()

        self.dummy_series = pd.Series(
            np.arange(1e3), index=np.array(np.arange(1e3), dtype="datetime64[ns]")
        )

        self.dummy_df = self.dummy_series.to_frame(name=1).reset_index(names=0)
        self.dummy_df[0] = self.dummy_df[0].astype("int64")

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

    @patch("datareservoirio.client.Storage")
    def test_init_with_cache_disabled(self, mock_storage):
        Client(self.auth, cache=False)
        assert not mock_storage.call_args.kwargs["cache"]

    @patch("datareservoirio.client.Storage")
    def test_init_with_cache_enabled(self, mock_storage):
        Client(self.auth, cache=True)
        assert mock_storage.call_args.kwargs["cache"]

    @patch("datareservoirio.client.Storage")
    def test_init_with_cache_format_verify_ignored(self, mock_storage):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            Client(self.auth, cache=True, cache_opt={"format": "csv"})
            assert mock_storage.call_args.kwargs["cache"]
            assert "format" not in mock_storage.call_args.kwargs["cache_opt"]

            assert len(w) == 1
            assert issubclass(w[-1].category, FutureWarning)

    @patch("datareservoirio.client.Storage")
    def test_init_with_cache_root(self, mock_storage):
        Client(self.auth, cache=True, cache_opt={"cache_root": "a:\\diskett"})
        assert mock_storage.call_args.kwargs["cache"]
        assert mock_storage.call_args.kwargs["cache_opt"] == {
            "cache_root": "a:\\diskett"
        }

    @patch("datareservoirio.client.Storage")
    def test_init_with_cache_max_size(self, mock_storage):
        Client(self.auth, cache=True, cache_opt={"max_size": 10})
        assert mock_storage.call_args.kwargs["cache"]
        assert mock_storage.call_args.kwargs["cache_opt"] == {"max_size": 10}

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
        self._storage.put = Mock()
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.create_with_data.return_value = expected_response
        mock_response = Mock()
        mock_response.json.return_value = self.dummy_params
        self.auth.post.return_value = mock_response

        response = self.client.create(self.dummy_series)

        args, kwargs = self._storage.put.call_args
        pd.testing.assert_frame_equal(args[0], self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params["FileId"]
        )
        self.client._timeseries_api.create_with_data.assert_called_once_with(
            self.dummy_params["FileId"]
        )
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_create_with_data_without_wait_on_verification(self, mock_sleep):
        self._storage.put = Mock()
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.create_with_data.return_value = expected_response
        mock_response = Mock()
        mock_response.json.return_value = self.dummy_params
        self.auth.post.return_value = mock_response

        response = self.client.create(self.dummy_series, wait_on_verification=False)

        args, kwargs = self._storage.put.call_args
        pd.testing.assert_frame_equal(args[0], self.dummy_df)
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
        self._storage.put = Mock()
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.add.return_value = expected_response
        mock_response = Mock()
        mock_response.json.return_value = self.dummy_params
        self.auth.post.return_value = mock_response

        response = self.client.append(self.dummy_series, self.timeseries_id)

        args, kwargs = self._storage.put.call_args
        pd.testing.assert_frame_equal(args[0], self.dummy_df)
        self.client._wait_until_file_ready.assert_called_once_with(
            self.dummy_params["FileId"]
        )

        self.client._timeseries_api.add.assert_called_once_with(
            self.timeseries_id, self.dummy_params["FileId"]
        )
        self.assertDictEqual(response, expected_response)

    @patch("time.sleep")
    def test_append_without_wait_on_verification(self, mock_sleep):
        self._storage.put = Mock()
        self.client._wait_until_file_ready = Mock(return_value="Ready")

        expected_response = {"abc": 123}
        self.client._timeseries_api.add.return_value = expected_response
        mock_response = Mock()
        mock_response.json.return_value = self.dummy_params
        self.auth.post.return_value = mock_response

        response = self.client.append(
            self.dummy_series, self.timeseries_id, wait_on_verification=False
        )

        args, kwargs = self._storage.put.call_args
        pd.testing.assert_frame_equal(args[0], self.dummy_df)
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

    # def test_get_with_defaults(self):
    #     index = np.array([1, 2, 3, 4, 5, 6])
    #     values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])

    #     self._storage.get.return_value = pd.DataFrame(
    #         {"index": index, "values": values}
    #     )
    #     response_expected = pd.Series(
    #         values, index=pd.to_datetime(index, utc=True), name="values"
    #     )

    #     response = self.client.get(self.timeseries_id)

    #     self.client._storage.get.assert_called_once_with(
    #         self.timeseries_id,
    #         datareservoirio.client._START_DEFAULT,
    #         datareservoirio.client._END_DEFAULT - 1,
    #     )
    #     pd.testing.assert_series_equal(response, response_expected)

    def test_get_with_convert_date_returns_series(self):
        index = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])

        series_with_dt = pd.Series(
            values[:-1], index=pd.to_datetime(index[:-1], utc=True), name="values"
        )

        start = pd.to_datetime(1, dayfirst=True, unit="ns", utc=True).value
        end = pd.to_datetime(10, dayfirst=True, unit="ns", utc=True).value

        self.client._storage.get.return_value = pd.DataFrame(
            {"index": index, "values": values}
        )
        response = self.client.get(self.timeseries_id, start, end, convert_date=True)

        self.client._storage.get.assert_called_once_with(
            f"https://reservoir-api-qa.4subsea.net/api/timeseries/{self.timeseries_id}/data/days?start=0&end=0"
            )

        pd.testing.assert_series_equal(response, series_with_dt, check_index_type=True)

    def test_get_without_convert_date_returns_series(self):
        index = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])

        series_with_dt = pd.Series(values[:-1], index=index[:-1], name="values")

        start = pd.to_datetime(1, dayfirst=True, unit="ns", utc=True).value
        end = pd.to_datetime(10, dayfirst=True, unit="ns", utc=True).value

        self.client._storage.get.return_value = pd.DataFrame(
            {"index": index, "values": values}
        )
        response = self.client.get(self.timeseries_id, start, end, convert_date=False)

        self.client._storage.get.assert_called_once_with(
            f"https://reservoir-api-qa.4subsea.net/api/timeseries/{self.timeseries_id}/data/days?start=0&end=0"
            )

        pd.testing.assert_series_equal(response, series_with_dt, check_index_type=True)

    def test_get_with_start_stop_as_str_calls_storagewithnanonsinceepoch(self):
        index = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        self._storage.get.return_value = pd.DataFrame(
            {"index": index, "values": values}
        )

        self.client.get(
            self.timeseries_id,
            start="1970-01-01 00:00:00.000000001",
            end="1970-01-01 00:00:00.000000004",
        )

        self.client._storage.get.assert_called_once_with(
            f"https://reservoir-api-qa.4subsea.net/api/timeseries/{self.timeseries_id}/data/days?start=0&end=0"
            )

    def test_get_with_emptytimeseries_return_empty(self):
        index = np.array([10, 11, 12])
        values = np.array([1, 2, 3])
        self._storage.get.return_value = pd.DataFrame(
            {"index": index, "values": values}
        )

        response_expected = pd.Series(dtype="float64", name="values")
        response_expected.index = pd.to_datetime(response_expected.index, utc=True)

        response = self.client.get(
            self.timeseries_id,
            start="1970-01-01 00:00:00.000000001",
            end="1970-01-01 00:00:00.000000004",
            raise_empty=False,
        )

        pd.testing.assert_series_equal(response, response_expected, check_dtype=False)

    def test_get_empty(self):
        self._storage.get.return_value = pd.DataFrame(
            columns=("index", "values")
        ).astype({"index": "int64"})
        response_expected = pd.Series(name="values", dtype="object")
        response_expected.index = pd.to_datetime(response_expected.index, utc=True)

        response = self.client.get(self.timeseries_id, 10, 20)

        self.client._storage.get.assert_called_once_with(
            f"https://reservoir-api-qa.4subsea.net/api/timeseries/{self.timeseries_id}/data/days?start=0&end=0"
            )
        pd.testing.assert_series_equal(response, response_expected)

    def test_get_with_raise_empty_throws(self):
        index = np.array([6, 7, 8, 9, 10])
        values = np.array([6.0, 7.0, 8.0, 9.0, 10.0])
        self._storage.get.return_value = pd.DataFrame(
            {"index": index, "values": values}
        )

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
        index = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])

        self._storage.get.return_value = pd.DataFrame(
            {"index": index, "values": values}
        )

        self.client.get(
            self.timeseries_id,
            start="1970-01-01 00:00:00.000000001",
            end="1970-01-01 00:00:00.000000004",
        )

        self.client._storage.get.assert_called_once_with(
            "https://reservoir-api-qa.4subsea.net/api/timeseries/abc-123-xyz/data/days?start=0&end=0")

    def test_search(self):
        self.client.search("test_namespace", "test_key", "test_name", 123)
        self.client._timeseries_api.search.assert_called_once_with(
            "test_namespace", "test_key", "test_name", 123
        )

    def test_search_None(self):
        with pytest.warns():
            self.client.search("test_namespace", None, "test_name", 123)

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

    def test_metadata_search(self):
        self.client.metadata_search(namespace="test_namespace", key="test_key")
        self.client._metadata_api.search.assert_called_once_with(
            "test_namespace", "test_key"
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

    def test_daterange(self):
        index = pd.date_range("2023", periods=10, freq="1S")
        values = np.random.rand(10)
        series = pd.Series(values, index=index)
        df_expected = pd.DataFrame({0: index.values.astype("int64"), 1: values})

        result = self.client._verify_and_prepare_series(series)
        pd.testing.assert_frame_equal(df_expected, result)

    def test_datetime64(self):
        index = np.arange(0, 10e9, 1e9, dtype="int64")
        values = np.random.rand(10)
        series = pd.Series(values, index=index.astype("datetime64[ns]"))
        df_expected = pd.DataFrame({0: index, 1: values})

        result = self.client._verify_and_prepare_series(series)
        pd.testing.assert_frame_equal(df_expected, result)

    def test_int64(self):
        index = np.arange(0, 10e9, 1e9, dtype="int64")
        values = np.random.rand(10)
        series = pd.Series(values, index=index)
        df_expected = pd.DataFrame({0: index, 1: values})

        result = self.client._verify_and_prepare_series(series)
        pd.testing.assert_frame_equal(df_expected, result)

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
