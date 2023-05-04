import pytest
from pathlib import Path

import pandas as pd
from datareservoirio._utils import DataHandler


TEST_PATH = Path(__file__).parent

class Test_DataHandler:

    @pytest.fixture
    def series(self):
        index_list = (
            1640995215379000000,
            1640995219176000000,
            1640995227270000000,
            1640995267223000000,
            1640995271472000000,
        )
        values_list = (-0.2, -0.1, 0.2, 0.1, 1.2)
        return pd.Series(data=values_list, index=index_list, name="values")

    @pytest.fixture
    def data_handler(self, series):
        return DataHandler(series)

    def test__init__(self, series):
        data_handler = DataHandler(series)
        pd.testing.assert_series_equal(data_handler._series, series)

    def test__init__raises_type(self, series):
        df = series.reset_index()
        with pytest.raises(ValueError):
            DataHandler(df)

    def test__init__raises_name(self, series):
        series.name = "invalid-name"
        with pytest.raises(ValueError):
            DataHandler(series)

    def test__init__raises_index_name(self, series):
        series.index.name = "invalid-name"
        with pytest.raises(ValueError):
            DataHandler(series)

    def test_as_series(self, series, data_handler):
        series_out = data_handler.as_series()
        assert series_out is not series
        pd.testing.assert_series_equal(series_out, series)

    def test_as_dataframe(self, series, data_handler):
        df_out = data_handler.as_dataframe()
        df_expect = series.reset_index()
        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_from_csv(self, series):
        csv_path = TEST_PATH / "testdata" / "data_float.csv"
        data_handler = DataHandler.from_csv(csv_path)
        pd.testing.assert_series_equal(data_handler.as_series(), series)
