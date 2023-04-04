import io
from pathlib import Path
from unittest.mock import ANY, Mock

import pandas as pd
import pytest
import requests
from requests import HTTPError

import datareservoirio as drio

TEST_PATH = Path(__file__).parent


class DataHandler:
    """
    Handles conversion of data series.

    Parameters
    ----------
    series : pandas.Series
        Data as series.
    """

    def __init__(self, series):
        self._series = series

    def as_series(self):
        """Return data as a ``pandas.Series`` object."""
        return self._series.copy(deep=True)

    def as_dataframe(self):
        """Return data as a ``pandas.DataFrame`` object."""
        return self.as_series().reset_index()

    def as_binary_csv(self):
        """Return data as a binary string (representing tha data in CSV format)."""
        df = self.as_dataframe()
        with io.BytesIO() as fp:
            kwargs = {
                "header": False,
                "index": False,
                "encoding": "utf-8",
                "mode": "wb",
            }
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

        csv_file = (
            TEST_PATH.parent
            / "testdata"
            / "RESPONSE_CASES_GENERAL"
            / "example_drio_blob_file.csv"
        )
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

    def test__df_to_blob(self, data_float):
        blob_url = "http://example/blob/url"
        _ = drio.storage.storage._df_to_blob(data_float.as_dataframe(), blob_url)

    def test__df_to_blob_raises_series(self, data_float):
        with pytest.raises(ValueError):
            blob_url = "http://example/blob/url"
            _ = drio.storage.storage._df_to_blob(data_float.as_series(), blob_url)

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

        assert (
            mock_requests.call_args.kwargs["data"].memory == data_float.as_binary_csv()
        )


class Test_Storage:
    """
    Tests the ``datareservoirio.storage.Storage`` class.

    What is currently tested:
        * Partially tested ``__init__``.
        * Partially tested ``get``.

    TODO:
        * Test ``__init__`` with cache and cache options.
        * Test ``get`` with no data available.
        * Test ``get`` with more than one overlapping File.
    """

    @pytest.fixture
    def storage_no_cache(self, auth_session):
        return drio.storage.Storage(auth_session, cache=False, cache_opt=None)

    def test__init__(self, auth_session):
        storage = drio.storage.Storage(auth_session, cache=False, cache_opt=None)

        assert storage._storage_cache is None
        assert storage._session is auth_session

    def test_get_raise_for_status(self, storage_no_cache):
        with pytest.raises(HTTPError):
            _ = storage_no_cache.get("http://example/no/exist")

    def test_get(self, storage_no_cache):
        target_url = "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672703939999999999"
        df_out = storage_no_cache.get(target_url)

        df_expect = pd.read_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        pd.testing.assert_frame_equal(df_out, df_expect)
