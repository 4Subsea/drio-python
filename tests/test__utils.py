import pytest

import pandas as pd
from datareservoirio._utils import DataHandler


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
