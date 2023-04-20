import pytest

import pandas as pd
import datareservoirio as drio
from datareservoirio.storage.cache_engine import CacheIO
from datareservoirio._utils import DataHandler


class Test_CacheIO:

    @pytest.fixture
    def data_float(self):
        index_list = (
            1640995215379000000,
            1640995219176000000,
            1640995227270000000,
            1640995267223000000,
            1640995271472000000,
        )

        values_list = (-0.2, -0.1, 0.2, 0.1, 1.2)

        series = pd.Series(data=values_list, index=index_list, name="values")

        data_handler = DataHandler(series)

        return data_handler

    @pytest.fixture
    def data_string(self):
        index_list = (
            1640995215379000000,
            1640995219176000000,
            1640995227270000000,
            1640995267223000000,
            1640995271472000000,
        )

        values_list = ("foo", "bar", "baz", "foobar", "abcd")

        series = pd.Series(data=values_list, index=index_list, name="values")

        data_handler = DataHandler(series)

        return data_handler

    @pytest.mark.parametrize("data", ("data_float", "data_string"))
    def test__write(self, request, data, tmp_path):
        data = request.getfixturevalue(data)

        filepath = tmp_path / "foobar"

        assert not filepath.exists()

        df = data.as_dataframe()
        CacheIO._write(df, str(filepath))

        assert filepath.exists()
        df_from_file = pd.read_parquet(filepath)
        pd.testing.assert_frame_equal(df_from_file, df)
