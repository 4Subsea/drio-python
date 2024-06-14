import json
import os
import time
import types
from encodings.utf_8 import encode
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests import HTTPError, Response
from requests.exceptions import InvalidJSONError
from tenacity import RetryError

import datareservoirio as drio
from datareservoirio._logging import exceptions_logger
from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


def change_logging(self, msg, *args, exc_info=True, **kwargs):
    if kwargs["extra"]:
        self.was_called = True
    else:
        raise ValueError("Missing extra parameters")


def drioResponseWithNoFiles():
    response = Response()
    json_data = {"Files": []}
    response._content = json.dumps(json_data).encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    response.status_code = 200
    return response


def failed_connection_error(self, url, timeout):
    if hasattr(self, "call_count"):
        self.call_count = self.call_count + 1
    else:
        self.call_count = 1

    if self.call_count >= 3:
        return drioResponseWithNoFiles()
    else:
        raise ConnectionError()


def fail_with_invalid_json_error(self, url, timeout):
    if hasattr(self, "call_count"):
        self.call_count = self.call_count + 1
    else:
        self.call_count = 1

    if self.call_count >= 3:
        return drioResponseWithNoFiles()
    else:
        raise InvalidJSONError()


def _mock_blob_sequence_days(response_json):
    return {1: "file1", 2: "file2"}


class Test_Client:
    """
    Tests the ``datareservoirio.Client`` class.
    """

    @pytest.fixture
    def group1_data(self):
        data = DataHandler.from_csv(
            TEST_PATH / "testdata" / "response_cases" / "group1" / "dataframe.csv"
        )
        return data

    @pytest.fixture
    def group2_data(self):
        """Overlapping data"""
        data = DataHandler.from_csv(
            TEST_PATH / "testdata" / "response_cases" / "group2" / "dataframe.csv"
        )
        return data

    @pytest.fixture
    def client(self, auth_session):
        return drio.Client(auth_session, cache=False)

    @pytest.fixture
    def cache_root(self, tmp_path):
        return tmp_path / ".cache"

    @pytest.fixture
    def client_with_cache(self, auth_session, cache_root):
        cache_opt = {"max_size": 1024, "cache_root": cache_root}
        return drio.Client(auth_session, cache=True, cache_opt=cache_opt)

    def test__init__(self, auth_session, tmp_path):
        cache_opt = {
            "max_size": 1024,
            "cache_root": tmp_path / ".cache",
        }
        drio.Client(auth_session, cache=True, cache_opt=cache_opt)

    def client_error_handler(self, client, method):
        exceptions_logger.exception = types.MethodType(change_logging, client)
        client._auth_session.get = types.MethodType(method, client._auth_session)
        return client

    @pytest.fixture
    def client_with_connection_error(self, client):
        return self.client_error_handler(client, failed_connection_error)

    @pytest.fixture
    def client_with_invalid_json_error(self, client):
        return self.client_error_handler(client, fail_with_invalid_json_error)

    @pytest.mark.parametrize(
        "start, end",
        [
            (1672358400000000000, 1672703939999999999 + 1),
            ("2022-12-30T00:00:00+00:00", "2023-01-02T23:59:00+00:00"),
            (pd.to_datetime("2022-12-30T00:00"), pd.to_datetime("2023-01-02T23:59:00")),
            (None, None),
        ],
    )
    def test_get(self, mock_requests, client, start, end, group1_data, response_cases):
        response_cases.set("group1")

        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=False,
        )

        series_expect = group1_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)
        # Check that the correct HTTP request is made
        if start and end:
            request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672703939999999999"
        else:
            request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=-9214560000000000000&end=9214646399999999999"
        assert mock_requests.call_args_list[0].args[1] == request_url_expect

    def test_get_convert_date(self, client, group1_data, response_cases):
        response_cases.set("group1")

        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=1672358400000000000,
            end=1672703939999999999 + 1,
            convert_date=True,
        )

        series_expect = group1_data.as_series()
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_raise_empty(self, client):
        with pytest.raises(ValueError):
            client.get("e3d82cda-4737-4af9-8d17-d9dfda8703d0", raise_empty=True)

    def test_get_keyerror(self, client):
        series_id = "test_series_id"
        start = "2023-01-01"
        end = "2023-01-02"

        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {
            "Files": [
                {"Chunks": "file1"},
                {"Chunks": "file2"},
            ]  # mock files with correct structure
        }

        client._auth_session.get = MagicMock(return_value=response_mock)

        def mock_storage_get(blob_sequence_i):
            if blob_sequence_i == "file1":
                return pd.DataFrame(
                    {
                        "index": [1672358410000000000, 1672358400000000000],
                        "values": [100, 200],
                    }
                )
            elif blob_sequence_i == "file2":
                return pd.DataFrame(
                    {
                        "index": [1672358410000000000, 1672358420000000000],
                        "values": [200, 400],
                    }
                )
            else:
                raise ValueError("Unexpected blob_sequence_i value")

        client._storage.get = MagicMock(side_effect=mock_storage_get)

        with patch(
            "datareservoirio.client._blob_sequence_days",
            side_effect=_mock_blob_sequence_days,
        ):
            with patch("datareservoirio.logging.warning") as mock_logging_warning:
                result = client.get(series_id, start, end)
                mock_logging_warning.assert_called_once_with(
                    "The time series you requested is not properly ordered. The data will be sorted to attempt to resolve the issue. Please note that this operation may take some time."
                )
                assert isinstance(result, pd.Series)

    def test_get_raises_end_not_after_start(self, client):
        start = 1672358400000000000
        end = start - 1
        with pytest.raises(ValueError):
            client.get(
                "2fee7f8a-664a-41c9-9b71-25090517c275",
                start=start,
                end=end,
            )

    def test_get_empty(self, client, response_cases):
        response_cases.set("datareservoirio-api")
        series_out = client.get(
            "e3d82cda-4737-4af9-8d17-d9dfda8703d0",
            start=None,
            end=None,
            convert_date=True,
        )

        series_expect = pd.Series(name="values", dtype="object")
        series_expect.index.name = None
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_overlapping(self, client, group2_data, response_cases):
        response_cases.set("group2")

        series_out = client.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )

        series_expect = group2_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_with_cache(
        self,
        client_with_cache,
        cache_root,
        STOREFORMATVERSION,
        group2_data,
        response_cases,
    ):
        response_cases.set("group2")

        # Check that the cache folder is empty
        cache_path_expect = cache_root / STOREFORMATVERSION
        assert cache_path_expect.exists()
        assert len(list(cache_path_expect.iterdir())) == 0

        # Get data (and store in cache)
        series_out = client_with_cache.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )

        series_expect = group2_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)

        # Check that the cache folder now contains six files
        assert len(list(cache_path_expect.iterdir())) == 6

        # Get data (from cache)
        time_before_get = time.time()
        time.sleep(0.1)
        series_out = client_with_cache.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )
        time.sleep(0.1)
        time_after_get = time.time()
        for cache_file_i in cache_path_expect.iterdir():
            time_access_file_i = os.path.getatime(cache_file_i)  # last access time
            assert time_before_get < time_access_file_i < time_after_get

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_ping(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        ping_out = client.ping()

        ping_json = (
            TEST_PATH
            / "testdata"
            / "response_cases"
            / "datareservoirio_api"
            / "ping.json"
        )
        with open(ping_json, mode="r") as f:
            ping_expect = json.load(f)

        assert ping_out == ping_expect

    def test_info(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        info_out = client.info("2fee7f8a-664a-41c9-9b71-25090517c275")

        info_json = (
            TEST_PATH
            / "testdata"
            / "response_cases"
            / "datareservoirio_timeseries_api"
            / "info.json"
        )
        with open(info_json, mode="r") as f:
            info_expect = json.load(f)

        assert info_out == info_expect

    def test_search(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        search_out = client.search("foo.bar")

        search_expect = {
            "ae3fe5c0-5521-4738-9664-87ef6c112cd8": [
                "{'Namespace':'foo.bar','Key':'baz','Value':{'a':1,'b':'something','d':1234}}",
                "{'Namespace':'foo.bar','Key':'other','Value':{'a':1,'b':'something','d':1234}}",
            ],
            "34a89adf-90af-40bb-8a99-b9ea710f01fd": [
                "{'Namespace':'foo.bar','Key':'baz','Value':{'a':1,'b':'something','d':1234}}"
            ],
        }

        assert search_out == search_expect

    def test_search_key(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        search_out = client.search("foo.bar", key="baz")

        search_expect = {
            "ae3fe5c0-5521-4738-9664-87ef6c112cd8": [
                "{'Namespace':'foo.bar','Key':'baz','Value':{'a':1,'b':'something','d':1234}}",
            ],
            "34a89adf-90af-40bb-8a99-b9ea710f01fd": [
                "{'Namespace':'foo.bar','Key':'baz','Value':{'a':1,'b':'something','d':1234}}"
            ],
        }

        assert search_out == search_expect

    def test_search_key_name(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        search_out = client.search("foo.bar", key="baz", name="a")

        search_expect = {
            "ae3fe5c0-5521-4738-9664-87ef6c112cd8": [
                "{'Namespace':'foo.bar','Key':'baz','Value':{'a':1,'b':'something','d':1234}}",
            ],
            "34a89adf-90af-40bb-8a99-b9ea710f01fd": [
                "{'Namespace':'foo.bar','Key':'baz','Value':{'a':1,'b':'something','d':1234}}"
            ],
        }

        assert search_out == search_expect

    def test_search_key_name_value(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        search_out = client.search("foo.bar", key="baz", name="a", value="1")

        search_expect = [
            "ae3fe5c0-5521-4738-9664-87ef6c112cd8",
            "34a89adf-90af-40bb-8a99-b9ea710f01fd",
        ]

        assert search_out == search_expect

    def test_delete(self, client, mock_requests, response_cases):
        response_cases.set("datareservoirio-api")

        client.delete("7bd106dd-d87f-4504-a888-6aeaff1ec31f")

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/7bd106dd-d87f-4504-a888-6aeaff1ec31f"
        assert mock_requests.call_args.args[1] == request_url_expect

    def test_create(self, client, monkeypatch, response_cases):
        response_cases.set("datareservoirio-api")

        def uuid4_mock():
            return "9f74b0b1-54c2-4148-8854-5f78b81bb592"

        monkeypatch.setattr("datareservoirio.client.uuid4", uuid4_mock)

        create_out = client.create()

        create_expect = {"TimeSeriesId": "9f74b0b1-54c2-4148-8854-5f78b81bb592"}
        assert create_out == create_expect

    def test_create_with_data(
        self, client, data_float, mock_requests, bytesio_with_memory, response_cases
    ):
        response_cases.set("group3")

        create_out = client.create(
            series=data_float.as_series(), wait_on_verification=True
        )

        create_expect = {
            "FileId": "e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2",
            "TimeSeriesId": "d30519af-5035-4093-a425-dafd857ad0ef",
            "TimeOfFirstSample": 1640995215379000000,
            "TimeOfLastSample": 1640995271472000000,
        }

        assert create_out == create_expect

        # Check first HTTP call to /api/files/upload
        call_url = mock_requests.call_args_list[0].args[1]
        call_url_expect = "https://reservoir-api.4subsea.net/api/files/upload"
        assert call_url == call_url_expect

        # Check second HTTP call to blob
        call_url = mock_requests.call_args_list[1].kwargs["url"]
        call_url_expect = "https://reservoirprod.blob.core.windows.net/files/e4fb7a7e07964f6a8c79f39a3af66dd2?sv=2021-10-04&spr=https&se=2023-04-28T10%3A30%3A10Z&sr=b&sp=rw&sig=Clj4cdfu%2FWivUqhnsxShkmG8STLmnzcCLzDEniSQZZg%3D"
        assert call_url == call_url_expect
        call_data = mock_requests.call_args_list[1].kwargs["data"]
        assert call_data.memory == data_float.as_binary_csv()

        # Check third HTTP call to /api/files/commit
        call_url = mock_requests.call_args_list[2].kwargs["url"]
        call_url_expect = "https://reservoir-api.4subsea.net/api/files/commit"
        assert call_url == call_url_expect
        call_json = mock_requests.call_args_list[2].kwargs["json"]
        call_json_expect = {"FileId": "e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2"}
        assert call_json == call_json_expect

        # Check fourth HTTP call to /api/files/{id}/status
        call_url = mock_requests.call_args_list[3].args[1]
        call_url_expect = "https://reservoir-api.4subsea.net/api/files/e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2/status"
        assert call_url == call_url_expect

        # Check fifth HTTP call to /api/timeseries/create
        call_url = mock_requests.call_args_list[4].args[1]
        call_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/create"
        assert call_url == call_url_expect
        call_data = mock_requests.call_args_list[4].kwargs["data"]
        call_data_expect = {"FileId": "e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2"}
        assert call_data == call_data_expect

    def test_create_failed(self, client, data_float, response_cases):
        response_cases.set("group3-failed")

        create_out = client.create(
            series=data_float.as_series(), wait_on_verification=True
        )

        create_expect = "Failed"

        assert create_out == create_expect

    def test_create_upload_raises(self, client, data_float, response_cases):
        response_cases.set("group3-upload-raises")

        with pytest.raises(HTTPError):
            client.create(series=data_float.as_series(), wait_on_verification=True)

    def test_create_raises_valueerror_unsorted_index(self, client):
        data = pd.Series(
            [1, 2, 3],
            index=[
                pd.to_datetime("2022-04-04"),
                pd.to_datetime("2022-04-03"),
                pd.to_datetime("2022-04-05"),
            ],
        )
        with pytest.raises(ValueError) as e:
            client.create(data)
        assert (
            str(e.value)
            == "Index not sorted. Please sort series on index before creating a timeseries."
        )

    def test_append(
        self, client, data_float, mock_requests, bytesio_with_memory, response_cases
    ):
        response_cases.set("group3")

        series_id = "d30519af-5035-4093-a425-dafd857ad0ef"
        append_out = client.append(
            data_float.as_series(), series_id, wait_on_verification=True
        )

        append_expect = {
            "FileId": "ae7ef55f-6861-44b4-be06-a1f789221c93",
            "TimeSeriesId": "d30519af-5035-4093-a425-dafd857ad0ef",
            "TimeOfFirstSample": 1640995215379000000,
            "TimeOfLastSample": 1640995271472000000,
        }

        assert append_out == append_expect

        # Check first HTTP call to /api/files/upload
        call_url = mock_requests.call_args_list[0].args[1]
        call_url_expect = "https://reservoir-api.4subsea.net/api/files/upload"
        assert call_url == call_url_expect

        # Check second HTTP call to blob
        call_url = mock_requests.call_args_list[1].kwargs["url"]
        call_url_expect = "https://reservoirprod.blob.core.windows.net/files/e4fb7a7e07964f6a8c79f39a3af66dd2?sv=2021-10-04&spr=https&se=2023-04-28T10%3A30%3A10Z&sr=b&sp=rw&sig=Clj4cdfu%2FWivUqhnsxShkmG8STLmnzcCLzDEniSQZZg%3D"
        assert call_url == call_url_expect
        call_data = mock_requests.call_args_list[1].kwargs["data"]
        assert call_data.memory == data_float.as_binary_csv()

        # Check third HTTP call to /api/files/commit
        call_url = mock_requests.call_args_list[2].kwargs["url"]
        call_url_expect = "https://reservoir-api.4subsea.net/api/files/commit"
        assert call_url == call_url_expect
        call_json = mock_requests.call_args_list[2].kwargs["json"]
        call_json_expect = {"FileId": "e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2"}
        assert call_json == call_json_expect

        # Check fourth HTTP call to /api/files/{id}/status
        call_url = mock_requests.call_args_list[3].args[1]
        call_url_expect = "https://reservoir-api.4subsea.net/api/files/e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2/status"
        assert call_url == call_url_expect

        # Check fifth HTTP call to /api/timeseries/create
        call_url = mock_requests.call_args_list[4].args[1]
        call_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/add"
        assert call_url == call_url_expect
        call_data = mock_requests.call_args_list[4].kwargs["data"]
        call_data_expect = {
            "TimeSeriesId": "d30519af-5035-4093-a425-dafd857ad0ef",
            "FileId": "e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2",
        }
        assert call_data == call_data_expect

    def test_append_failed(self, client, data_float, response_cases):
        response_cases.set("group3-failed")

        series_id = "d30519af-5035-4093-a425-dafd857ad0ef"
        append_out = client.append(
            data_float.as_series(), series_id, wait_on_verification=True
        )

        append_expect = "Failed"

        assert append_out == append_expect

    def test_append_upload_raises(self, client, data_float, response_cases):
        response_cases.set("group3-upload-raises")

        series_id = "d30519af-5035-4093-a425-dafd857ad0ef"
        with pytest.raises(HTTPError):
            client.append(data_float.as_series(), series_id, wait_on_verification=True)

    def test_append_raises_valueerror_unsorted_index(self, client):
        series_id = "d30519af-5035-4093-a425-dafd857ad0ef"
        data = pd.Series(
            [1, 2, 3],
            index=[
                pd.to_datetime("2022-04-04"),
                pd.to_datetime("2022-04-03"),
                pd.to_datetime("2022-04-05"),
            ],
        )
        with pytest.raises(ValueError) as e:
            client.append(data, series_id)
        assert (
            str(e.value)
            == "Index not sorted. Please sort series on index before appending data."
        )

    @pytest.mark.parametrize("data", ("data_float", "data_string"))
    def test__verify_and_prepare_series(self, client, data, request):
        data = request.getfixturevalue(data)
        df_out = client._verify_and_prepare_series(data.as_series())
        df_expect = data.as_dataframe().rename(columns={"index": 0, "values": 1})
        pd.testing.assert_frame_equal(df_out, df_expect)

    def test__verify_and_prepare_series_raises_not_series(self, client, data_float):
        with pytest.raises(ValueError):
            client._verify_and_prepare_series(data_float.as_dataframe())

    def test__verify_and_prepare_series_raises_index(self, client, data_float):
        with pytest.raises(ValueError):
            client._verify_and_prepare_series(data_float.as_dataframe())

    def test__verify_and_prepare_series_raises_not_unique(self, client, data_float):
        series = data_float.as_series()
        index = list(series.index)
        index[1] = index[0]  # not unique index
        series.index = index
        with pytest.raises(ValueError):
            client._verify_and_prepare_series(data_float.as_dataframe())

    def test__get_file_status(self, client, response_cases):
        response_cases.set("group3")
        status_out = client._get_file_status("e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2")
        status_expect = "Ready"
        assert status_out == status_expect

    def test__get_file_status_failed(self, client, response_cases):
        response_cases.set("group3-failed")
        status_out = client._get_file_status("e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2")
        status_expect = "Failed"
        assert status_out == status_expect

    def test__wait_until_file_ready(self, client, response_cases):
        response_cases.set("group3")
        out = client._wait_until_file_ready("e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2")
        assert out == "Ready"

    def test__wait_until_file_ready_failed(self, client, response_cases):
        response_cases.set("group3-failed")
        out = client._wait_until_file_ready("e4fb7a7e-0796-4f6a-8c79-f39a3af66dd2")
        assert out == "Failed"

    def test_metadata_set(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        response = client.metadata_set(
            "foo.bar", "baz", vendor="Sensor Corp", type_="Ampermeter"
        )

        id_expect = "dd5945ad-67f5-499c-fea4-08db4d49f13b"
        assert response["Id"] == id_expect

        # Check that the correct URL is poked
        request_url_expect = (
            "https://reservoir-api.4subsea.net/api/metadata/foo.bar/baz?overwrite=true"
        )
        assert mock_requests.call_args.args[1] == request_url_expect

        # Check that the correct json is sent
        json_expect = {"Value": {"vendor": "Sensor Corp", "type_": "Ampermeter"}}
        assert mock_requests.call_args.kwargs["json"] == json_expect

    def test_metadata_get(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        response = client.metadata_get(namespace="foo.bar", key="baz")

        response_expect = {
            "Id": "19b7230b-f88a-4217-b1c9-08daff938054",
            "Namespace": "foo.bar",
            "Key": "baz",
            "Value": {"vendor": "Sensor Corp", "type_": "Ampermeter"},
            "LastModifiedByEmail": "user@4subsea.com",
            "LastModified": "2023-05-05T09:01:32.6706215+00:00",
            "Created": "2023-01-26T11:50:20.4812338+00:00",
            "CreatedByEmail": "user@4subsea.com",
        }

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = (
            "https://reservoir-api.4subsea.net/api/metadata/foo.bar/baz"
        )
        assert mock_requests.call_args.args[1] == request_url_expect

    def test_metadata_get_by_id(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        response = client.metadata_get(
            metadata_id="8620c8cf-d9db-4ec6-9393-d45a7df6c115"
        )

        response_expect = {
            "Id": "8620c8cf-d9db-4ec6-9393-d45a7df6c115",
            "Namespace": "foo.bar",
            "Key": "baz",
            "Value": {"vendor": "Sensor Corp", "type_": "Ampermeter" },
            "TimeSeries": [
                {
                    "TimeSeriesId": "fc485e50-e641-4410-bf10-8ce4b5d24405",
                    "TimeOfFirstSample": 0,
                    "TimeOfLastSample": -1,
                    "LastModifiedByEmail": "user@4subsea.com",
                    "Created": "2024-06-14T08:37:47.109Z",
                    "LastModified": "2024-06-14T08:37:47.109Z",
                    "CreatedByEmail": "user@4subsea.com"
                }
            ],
            "LastModifiedByEmail": "user@4subsea.com",
            "LastModified": "2023-05-05T09:01:32.6706215+00:00",
            "Created": "2023-01-26T11:50:20.4812338+00:00",
            "CreatedByEmail": "user@4subsea.com"
        }

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/metadata/8620c8cf-d9db-4ec6-9393-d45a7df6c115"
        assert mock_requests.call_args.args[1] == request_url_expect

    def test_metadata_get_raises(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        with pytest.raises(ValueError):
            client.metadata_get(metadata_id=None, namespace="foo.bar", key=None)

    def test_metadata_search(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        response = client.metadata_search("foo.bar", "baz")

        response_expect = [
            {
                "Id": "19b7230b-f88a-4217-b1c9-08daff938054",
                "Namespace": "foo.bar",
                "Key": "baz",
                "Value": {"vendor": "Sensor Corp", "type_": "Ampermeter"},
                "LastModifiedByEmail": "user@4subsea.com",
                "LastModified": "2023-05-05T09:01:32.6706215+00:00",
                "Created": "2023-01-26T11:50:20.4812338+00:00",
                "CreatedByEmail": "user@4subsea.com",
            }
        ]

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/metadata/search"
        assert mock_requests.call_args.args[1] == request_url_expect

        # Check that the correct json is sent
        json_expect = {"Namespace": "foo.bar", "Key": "baz", "Value": {}}
        assert mock_requests.call_args.kwargs["json"] == json_expect

    def test_metadata_browse_namespace(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")
        response = client.metadata_browse(namespace="foo.bar")
        response_expect = ["abcd", "baz"]
        assert response == response_expect
        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/metadata/foo.bar"
        assert mock_requests.call_args.args[1] == request_url_expect

    def test_metadata_browse(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        response = client.metadata_browse()

        response_expect = sorted(
            [
                "TheRig2",
                "TheRig3",
                "This is a new test",
                "This is a test",
                "This is a third test",
                "Tor_TEST",
                "TorEinarTest",
                "tress",
                "vessel.electrical",
                "vessel.electrical.rune",
            ]
        )

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/metadata/"
        assert mock_requests.call_args.args[1] == request_url_expect

    def test_metadata_delete(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")
        client.metadata_delete("19b7230b-f88a-4217-b1c9-08daff938054")

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/metadata/19b7230b-f88a-4217-b1c9-08daff938054"
        assert mock_requests.call_args.args[1] == request_url_expect

    def test_set_metadata(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        series_id = "857ca134-5bf7-4c14-b687-ede7d5cbf22f"
        metadata_id = "19b7230b-f88a-4217-b1c9-08daff938054"
        namevalues = {"vendor": "Sensor Corp", "type_": "Ampermeter"}
        response = client.set_metadata(
            series_id=series_id,
            metadata_id=metadata_id,
            overwrite=True,
            namevalues=namevalues,
        )

        response_expect = {
            "TimeSeriesId": "857ca134-5bf7-4c14-b687-ede7d5cbf22f",
            "TimeOfFirstSample": 0,
            "TimeOfLastSample": -1,
            "LastModifiedByEmail": "user@4subsea.com",
            "Created": "2023-05-03T10:25:44.572Z",
            "LastModified": "2023-05-03T10:25:44.572Z",
            "CreatedByEmail": "user@4subsea.com",
            "Metadata": [
                {
                    "Id": "8dc03b22-5a7c-499b-8861-fed5902e9f91",
                    "Namespace": "ns",
                    "Key": "key",
                    "Value": {
                        "some_name": "tress",
                        "some_value": "emerald"
                    },
                    "LastModifiedByEmail": "user@4subsea.com",
                    "LastModified": "2023-05-03T10:25:44.572Z",
                    "Created": "2023-05-03T10:25:44.572Z",
                    "CreatedByEmail": "user@4subsea.com",
                }
            ]
        }

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/857ca134-5bf7-4c14-b687-ede7d5cbf22f/metadata"
        assert mock_requests.call_args.args[1] == request_url_expect

        # Check that the correct json with metadata id is sent
        json_expect = ["19b7230b-f88a-4217-b1c9-08daff938054"]
        assert mock_requests.call_args.kwargs["json"] == json_expect

    def test_set_metadata_ns_key(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")

        series_id = "857ca134-5bf7-4c14-b687-ede7d5cbf22f"
        namevalues = {"vendor": "Sensor Corp", "type_": "Ampermeter"}
        response = client.set_metadata(
            series_id=series_id,
            namespace="foo.bar",
            key="baz",
            overwrite=True,
            namevalues=namevalues,
        )

        response_expect = {
            "TimeSeriesId": "857ca134-5bf7-4c14-b687-ede7d5cbf22f",
            "TimeOfFirstSample": 0,
            "TimeOfLastSample": -1,
            "LastModifiedByEmail": "user@4subsea.com",
            "Created": "2023-05-03T10:25:44.572Z",
            "LastModified": "2023-05-03T10:25:44.572Z",
            "CreatedByEmail": "user@4subsea.com",
            "Metadata": [
                {
                    "Id": "8dc03b22-5a7c-499b-8861-fed5902e9f91",
                    "Namespace": "ns",
                    "Key": "key",
                    "Value": {
                        "some_name": "tress",
                        "some_value": "emerald"
                    },
                    "LastModifiedByEmail": "user@4subsea.com",
                    "LastModified": "2023-05-03T10:25:44.572Z",
                    "Created": "2023-05-03T10:25:44.572Z",
                    "CreatedByEmail": "user@4subsea.com",
                }
            ]
        }

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/857ca134-5bf7-4c14-b687-ede7d5cbf22f/metadata"
        assert mock_requests.call_args.args[1] == request_url_expect

        # Check that the correct json with metadata id is sent
        json_expect = ["dd5945ad-67f5-499c-fea4-08db4d49f13b"]
        assert mock_requests.call_args.kwargs["json"] == json_expect

    def test_set_metadata_raises_no_id_and_namespace(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        series_id = "857ca134-5bf7-4c14-b687-ede7d5cbf22f"
        namevalues = {"vendor": "Sensor Corp", "type_": "Ampermeter"}
        with pytest.raises(ValueError):
            client.set_metadata(
                series_id,
                metadata_id=None,
                namespace=None,
                key="baz",
                overwrite=True,
                namevalues=namevalues,
            )

    def test_set_metadata_raises_namespace_and_no_key(self, client, response_cases):
        response_cases.set("datareservoirio-api")

        series_id = "857ca134-5bf7-4c14-b687-ede7d5cbf22f"
        namevalues = {"vendor": "Sensor Corp", "type_": "Ampermeter"}
        with pytest.raises(ValueError):
            client.set_metadata(
                series_id,
                metadata_id=None,
                namespace="foo.bar",
                key=None,
                overwrite=True,
                namevalues=namevalues,
            )

    def test_remove_metadata(self, client, response_cases, mock_requests):
        response_cases.set("datareservoirio-api")
        series_id = "857ca134-5bf7-4c14-b687-ede7d5cbf22f"
        metadata_id = "19b7230b-f88a-4217-b1c9-08daff938054"
        response = client.remove_metadata(series_id, metadata_id)
        response_expect = {
            "TimeSeriesId": "857ca134-5bf7-4c14-b687-ede7d5cbf22f",
            "TimeOfFirstSample": 0,
            "TimeOfLastSample": -1,
            "LastModifiedByEmail": "user@4subsea.com",
            "Created": "2023-05-03T10:25:44.567Z",
            "LastModified": "2023-05-03T10:25:44.567Z",
            "CreatedByEmail": "user@4subsea.com",
            "Metadata": [
                {
                    "Id": "8dc03b22-5a7c-499b-8861-fed5902e9f91",
                    "Namespace": "ns",
                    "Key": "key",
                    "Value": {
                        "some_name": "tress",
                        "some_value": "emerald"
                    },
                    "LastModifiedByEmail": "user@4subsea.com",
                    "LastModified": "2023-05-03T10:25:44.567Z",
                    "Created": "2023-05-03T10:25:44.567Z",
                    "CreatedByEmail": "user@4subsea.com",
                }
            ]
        }

        assert response == response_expect

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/857ca134-5bf7-4c14-b687-ede7d5cbf22f/metadata"
        assert mock_requests.call_args.args[1] == request_url_expect

        # Check that the correct json with metadata id is sent
        json_expect = ["19b7230b-f88a-4217-b1c9-08daff938054"]
        assert mock_requests.call_args.kwargs["json"] == json_expect

    def test_client_get_throws_exception_is_logged(self, client):
        client.was_called = False
        exceptions_logger.exception = types.MethodType(change_logging, client)
        with pytest.raises(ValueError):
            client.get("e3d82cda-4737-4af9-8d17-d9dfda8703d0", raise_empty=True)
        assert client.was_called == True

    def test_client_retries_on_connection_errors(self, client_with_connection_error):
        client_with_connection_error.get("e3d82cda-4737-4af9-8d17-d9dfda8703d0")

        attempts_from_tenacity = client_with_connection_error.get.retry.statistics[
            "attempt_number"
        ]
        call_count_from_fake_auth_counter = (
            client_with_connection_error._auth_session.call_count
        )

        assert attempts_from_tenacity == 3
        assert attempts_from_tenacity == call_count_from_fake_auth_counter
        assert call_count_from_fake_auth_counter == 3

    def test_tries_error_does_not_throw_retry(self, client_with_invalid_json_error):
        with pytest.raises(InvalidJSONError) as ex:
            client_with_invalid_json_error.get("e3d82cda-4737-4af9-8d17-d9dfda8703d0")

        attempts = client_with_invalid_json_error.get.retry.statistics["attempt_number"]
        assert attempts == 1

    @pytest.mark.response_irrelevant
    @pytest.mark.parametrize(
        "aggregation_function, expected",
        [("mean", "Avg"), ("std", "Stdev"), ("min", "Min"), ("max", "Max")],
    )
    def test_aggregation_function_gets_translated(
        self, client, mock_requests, aggregation_function, expected, response_cases
    ):
        response_cases.set("datareservoirio-api")

        client.get_samples_aggregate(
            "e3d82cda-4737-4af9-8d17-d9dfda8703d0",
            start="2023-12-01",
            end="2023-12-02",
            aggregation_period="15m",
            aggregation_function=aggregation_function,
        )

        request_url = mock_requests.call_args.args[1]
        assert f"aggregationFunction={expected}" in request_url

    @pytest.mark.response_irrelevant
    @pytest.mark.parametrize(
        "aggregation_period, expected",
        [
            ("min", "1m"),
            ("tick", "1tick"),
            ("s", "1s"),
            ("15minutes", "15m"),
            ("15minute", "15m"),
            ("15min", "15m"),
            ("15hours", "15h"),
            ("15hour", "15h"),
            ("15hr", "15h"),
            ("15seconds", "15s"),
            ("15second", "15s"),
            ("15sec", "15s"),
            ("15milliseconds", "15ms"),
            ("15millisecond", "15ms"),
            ("15millis", "15ms"),
            ("15milli", "15ms"),
            ("15microseconds", "15microsecond"),
            ("15micros", "15microsecond"),
            ("15micro", "15microsecond"),
        ],
    )
    def test_aggregation_period(
        self,
        monkeypatch,
        client,
        mock_requests,
        aggregation_period,
        expected,
        response_cases,
    ):
        response_cases.set("datareservoirio-api")
        client.get_samples_aggregate(
            "e3d82cda-4737-4af9-8d17-d9dfda8703d0",
            start="2023-12-01",
            end="2023-12-02",
            aggregation_period=aggregation_period,
            aggregation_function="mean",
        )

        request_url = mock_requests.call_args.args[1]
        assert f"aggregationPeriod={expected}" in request_url
