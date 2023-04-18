from pathlib import Path

import pandas as pd
import pytest

import datareservoirio as drio

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
    def client(self, auth_session):
        return drio.Client(auth_session, cache=False)

    def test__init__(self, auth_session):
        drio.Client(auth_session, cache=False)

    def test_get(self, client):
        start = 1672358400000000000
        end = 1672703940000000000
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=True,
        )

        df_expect = pd.read_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        series_expect = df_expect.set_index("index").squeeze("columns")
        series_expect.index.name = None
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_no_convert(self, client):
        start = 1672358400000000000
        end = 1672703940000000000
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=False,
        )

        df_expect = pd.read_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        series_expect = df_expect.set_index("index").squeeze("columns")
        series_expect.index.name = None

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_start_end_as_string(self, client):
        start = "2022-12-30T00:00:00+00:00"
        end = "2023-01-02T23:59:00+00:00"
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=True,
        )

        df_expect = pd.read_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        series_expect = df_expect.set_index("index").squeeze("columns")
        series_expect.index.name = None
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_start_end_as_datetime(self, client):
        start = pd.to_datetime("2022-12-30T00:00:00+00:00")
        end = pd.to_datetime("2023-01-02T23:59:00+00:00")
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=True,
        )

        df_expect = pd.read_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        series_expect = df_expect.set_index("index").squeeze("columns")
        series_expect.index.name = None
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_start_end_as_none(self, client):
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=None,
            end=None,
            convert_date=True,
        )

        df_expect = pd.read_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        series_expect = df_expect.set_index("index").squeeze("columns")
        series_expect.index.name = None
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    # def test_get_empty(self, client):
    #     series_out = client.get(
    #         "2fee7f8a-664a-41c9-9b71-25090517c275",
    #         start=None,
    #         end=None,
    #         convert_date=True,
    #     )

    #     df_expect = pd.read_csv(
    #         TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
    #         header=None,
    #         names=("index", "values"),
    #         dtype={"index": "int64", "values": "float64"},
    #         encoding="utf-8",
    #     )

    #     series_expect = df_expect.set_index("index").squeeze("columns")
    #     series_expect.index.name = None
    #     series_expect.index = pd.to_datetime(series_expect.index, utc=True)

    #     pd.testing.assert_series_equal(series_out, series_expect)
