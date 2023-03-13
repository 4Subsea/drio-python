import io
from pathlib import Path
from unittest.mock import ANY, Mock

import pandas as pd
import pytest
import requests

import datareservoirio as drio

TEST_PATH = Path(__file__).parent


class DataHandler:
    def __init__(self, series):
        self._series = series

    def as_series(self):
        return self._series.copy(deep=True)

    def as_dataframe(self):
        return self.as_series().reset_index()

    def as_binary_csv(self):
        df = self.as_dataframe()
        with io.BytesIO() as fp:
            kwargs = {"header": False, "index": False, "encoding": "utf-8", "mode": "wb"}
            df.to_csv(fp, lineterminator="\n", **kwargs)
            csv = fp.getvalue()
        return csv


class Test__blob_to_df:
    """
    Tests the :func:`_blob_to_df` function.

    TODO: add one test where the blob file contains string values.
    """

    def test__blob_to_df(self):
        """Tests ``_blob_to_df`` function."""
        blob_url = "http://example/drio/blob/file"
        df_out = drio.storage.storage._blob_to_df(blob_url)

        csv_file = TEST_PATH.parent / "testdata" / "example_drio_blob_file.csv"
        df_expect = pd.read_csv(
            csv_file,
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "str"},
            encoding="utf-8",
        ).astype({"values": "float64"}, errors="ignore")

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_raise_for_status(self):
        """Tests if ``raise_for_status`` is called"""
        with pytest.raises(requests.HTTPError):
            blob_url = "http://example/no/exist"
            _ = drio.storage.storage._blob_to_df(blob_url)


class Test__df_to_blob:
    """
    Tests the :func:`_df_to_blob` function.

    TODO: Test for DataFrame with string values.
    """

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

    # @pytest.fixture
    # def df_float(self):
    #     """DataFrame with float values"""
    #     index_list = (
    #         1640995215379000000,
    #         1640995219176000000,
    #         1640995227270000000,
    #         1640995267223000000,
    #         1640995271472000000,
    #     )

    #     values_list = (-0.2, -0.1, 0.2, 0.1, 1.2)

    #     df = pd.DataFrame(data={"index": index_list, "values": values_list}).astype(
    #         {"index": "int64", "values": "float64"}
    #     )

    #     return df

    # @pytest.fixture
    # def csv_float_expect(self):
    #     """Binary csv based on DataFrame with float values"""
    #     return (
    #         b"1640995215379000000,-0.2\n"
    #         b"1640995219176000000,-0.1\n"
    #         b"1640995227270000000,0.2\n"
    #         b"1640995267223000000,0.1\n"
    #         b"1640995271472000000,1.2\n"
    #     )

    def test__df_to_blob(self, data_float):
        blob_url = "http://example/blob/url"
        _ = drio.storage.storage._df_to_blob(data_float.as_dataframe(), blob_url)

    def test__df_to_blob_raises_series(self, data_float):
        with pytest.raises(ValueError):
            blob_url = "http://example/blob/url"
            _ = drio.storage.storage._df_to_blob(data_float.as_series(), blob_url)

    # def test__df_to_blob_call_args(self, monkeypatch, df_float, csv_float_expect):
    #     mock_response = Mock()

    #     def put_side_effect(*args, **kwargs):
    #         if data := kwargs.get("data"):
    #             assert data.read() == csv_float_expect
    #         return mock_response

    #     mock_put = Mock(side_effect=put_side_effect)

    #     monkeypatch.setattr(requests, "put", mock_put)

    #     blob_url = "http://foo/bar/baz"
    #     _ = drio.storage.storage._df_to_blob(df_float, blob_url)

    #     mock_put.assert_called_once_with(
    #         blob_url,
    #         headers={"x-ms-blob-type": "BlockBlob"},
    #         data=ANY,
    #     )

    #     mock_response.raise_for_status.assert_called_once()

    def test__df_to_blob_call_args_2(
        self, mock_requests, bytesio_with_memory, data_float
    ):
        blob_url = "http://example/blob/url"
        _ = drio.storage.storage._df_to_blob(data_float.as_dataframe(), blob_url)

        mock_requests.assert_called_once_with(
            method="put",
            url=blob_url,
            headers={"x-ms-blob-type": "BlockBlob"},
            data=ANY,
        )

        assert mock_requests.call_args.kwargs["data"].memory == data_float.as_binary_csv()
