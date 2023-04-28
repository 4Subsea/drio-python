import os
import time
from pathlib import Path

import pandas as pd
import pytest
import json

import datareservoirio as drio
from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


class Test_Client:
    """
    Tests the ``datareservoirio.Client`` class.
    """

    @pytest.fixture
    def group1_data(self):
        data = DataHandler.from_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv"
        )
        return data

    @pytest.fixture
    def group2_data(self):
        """Overlapping data"""
        data = DataHandler.from_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "dataframe.csv"
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

    @pytest.mark.parametrize(
        "start, end",
        [
            (1672358400000000000, 1672703939999999999 + 1),
            ("2022-12-30T00:00:00+00:00", "2023-01-02T23:59:00+00:00"),
            (pd.to_datetime("2022-12-30T00:00"), pd.to_datetime("2023-01-02T23:59:00")),
            (None, None),
        ],
    )
    def test_get(self, mock_requests, client, start, end, group1_data):
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=False,
        )

        series_expect = group1_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)
        # Check that the correct HTTP request is made
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672703939999999999"
        mock_requests.call_args_list[0].kwargs["url"] = request_url_expect

    def test_get_convert_date(self, client, group1_data):
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

    def test_get_raises_end_not_after_start(self, client):
        start = 1672358400000000000
        end = start - 1
        with pytest.raises(ValueError):
            client.get(
                "2fee7f8a-664a-41c9-9b71-25090517c275",
                start=start,
                end=end,
            )

    def test_get_empty(self, client):
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

    def test_get_overlapping(self, client, group2_data):
        series_out = client.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )

        series_expect = group2_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_with_cache(
        self, client_with_cache, cache_root, STOREFORMATVERSION, group2_data
    ):
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

    def test_ping(self, client):
        ping_out = client.ping()

        ping_json = TEST_PATH / "testdata" / "RESPONSE_CASES_GENERAL" / "ping.json"
        with open(ping_json, mode="r") as f:
            ping_expect = json.load(f)

        assert ping_out == ping_expect

    def test_info(self, client):
        info_out = client.info("2fee7f8a-664a-41c9-9b71-25090517c275")

        info_json = TEST_PATH / "testdata" / "RESPONSE_CASES_GENERAL" / "info.json"
        with open(info_json, mode="r") as f:
            info_expect = json.load(f)

        assert info_out == info_expect

    def test_delete(self, client, mock_requests):
        client.delete("7bd106dd-d87f-4504-a888-6aeaff1ec31f")

        # Check that the correct URL is poked
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/7bd106dd-d87f-4504-a888-6aeaff1ec31f"
        mock_requests.call_args.kwargs["url"] = request_url_expect

    def test_create(self, client, monkeypatch):
        def uuid4_mock():
            return "9f74b0b1-54c2-4148-8854-5f78b81bb592"
        monkeypatch.setattr(drio.rest_api.timeseries, "uuid4", uuid4_mock)

        create_out = client.create()

        create_expect = {"TimeSeriesId": "9f74b0b1-54c2-4148-8854-5f78b81bb592"}
        assert create_out == create_expect
