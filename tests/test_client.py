from pathlib import Path

import pandas as pd
import pytest

import datareservoirio as drio
from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


class Test_Client:
    """
    Tests the ``datareservoirio.Client`` class.

    What is currently tested:
        * Tested ``__init__``.
        * Partially tested ``get``.

    TODO:
        * Test ``get`` with start/end as None.
        * Test ``get`` with empty data.
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

    def test__init__(self, auth_session, tmp_path):
        cache_opt = {
            "max_size": 1024,
            "cache_root": tmp_path / ".cache",
        }
        drio.Client(auth_session, cache=True, cache_opt=cache_opt)

    @pytest.mark.parametrize(
        "start, end",
        [
            (1672358400000000000, 1672703940000000000),
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
            end=1672703940000000000,
            convert_date=True,
        )

        series_expect = group1_data.as_series()
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

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

    def test_get_raise_empty(self, client):
        with pytest.raises(ValueError):
            client.get("e3d82cda-4737-4af9-8d17-d9dfda8703d0", raise_empty=True)

    def test_get_overlapping(self, client, group2_data):
        series_out = client.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )

        series_expect = group2_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)
