import io
from pathlib import Path

import pandas as pd
import pytest

from datareservoirio._utils import DataHandler, _check_malformatted

TEST_PATH = Path(__file__).parent


class Test_DataHandler:
    @pytest.fixture
    def series_float(self):
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
    def series_string(self):
        index_list = (
            1640995215379000000,
            1640995219176000000,
            1640995227270000000,
            1640995267223000000,
            1640995271472000000,
        )
        values_list = ("foo", "bar", "baz", "foobar", "abcd")
        return pd.Series(data=values_list, index=index_list, name="values")

    @pytest.fixture
    def series_string_malformatted(self):
        index_list = (
            1640995215379000000,
            1640995219176000000,
            1640995227270000000,
            1640995267223000000,
            1640995271472000000,
        )
        values_list = ("fo,o", "bar", "b,a,z", "foo,bar", "ab,cd")
        return pd.Series(data=values_list, index=index_list, name="values")

    @pytest.fixture
    def data_handler_float(self, series_float):
        return DataHandler(series_float)

    @pytest.mark.parametrize(
        "series", ("series_float", "series_string", "series_string_malformatted")
    )
    def test__init__(self, request, series):
        series = request.getfixturevalue(series)
        data_handler = DataHandler(series)
        pd.testing.assert_series_equal(data_handler._series, series)

    def test__init__raises_type(self, series_float):
        df = series_float.reset_index()
        with pytest.raises(ValueError):
            DataHandler(df)

    def test__init__raises_name(self, series_float):
        series_float.name = "invalid-name"
        with pytest.raises(ValueError):
            DataHandler(series_float)

    def test__init__raises_index_name(self, series_float):
        series_float.index.name = "invalid-name"
        with pytest.raises(ValueError):
            DataHandler(series_float)

    def test_as_series(self, series_float, data_handler_float):
        series_out = data_handler_float.as_series()
        assert series_out is not series_float
        pd.testing.assert_series_equal(series_out, series_float)

    def test_as_dataframe(self, series_float, data_handler_float):
        df_out = data_handler_float.as_dataframe()
        df_expect = series_float.reset_index()
        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_as_binary_csv(self, data_handler_float):
        csv_out = data_handler_float.as_binary_csv()
        csv_expect = b"1640995215379000000,-0.2\n1640995219176000000,-0.1\n1640995227270000000,0.2\n1640995267223000000,0.1\n1640995271472000000,1.2\n"
        assert csv_out == csv_expect

    @pytest.mark.parametrize(
        "csv_path, series",
        [
            ("data_float.csv", "series_float"),
            ("data_string.csv", "series_string"),
            ("data_string_malformatted.csv", "series_string_malformatted"),
        ],
    )
    def test_from_csv(self, request, csv_path, series):
        series = request.getfixturevalue(series)
        csv_path = TEST_PATH / "testdata" / csv_path
        data_handler = DataHandler.from_csv(csv_path)
        pd.testing.assert_series_equal(data_handler.as_series(), series)


class Test__check_malformatted:
    @pytest.mark.parametrize(
        "filename, expect",
        [
            ("data_float.csv", False),
            ("data_string.csv", False),
            ("data_string_malformatted.csv", True),
        ],
    )
    def test__check_malformatted_file(self, filename, expect):
        filepath = TEST_PATH / "testdata" / filename
        out = _check_malformatted(filepath)
        assert out == expect

    @pytest.mark.parametrize(
        "filename, expect",
        [
            ("data_float.csv", False),
            ("data_string.csv", False),
            ("data_string_malformatted.csv", True),
        ],
    )
    def test__check_malformatted_stream(self, filename, expect):
        filepath = TEST_PATH / "testdata" / filename
        with io.BytesIO() as stream:
            with open(filepath, mode="rb") as f:
                stream.write(f.read())
            stream.seek(0)
            out = _check_malformatted(stream)
            assert out == expect
