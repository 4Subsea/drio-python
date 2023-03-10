from pathlib import Path
from unittest.mock import ANY, Mock

import pandas as pd
import pytest
import requests

import datareservoirio as drio

TEST_PATH = Path(__file__).parent


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
    TODO:
        * Test for DataFrame with string values.
        * Check that raises ValueError if df is not DataFrame
    """

    @pytest.fixture
    def df_float(self):
        """DataFrame with float values"""
        index_list = (
            1640995215379000000,
            1640995219176000000,
            1640995227270000000,
            1640995267223000000,
            1640995271472000000,
        )

        values_list = (-0.2, -0.1, 0.2, 0.1, 1.2)

        df = pd.DataFrame(data={"index": index_list, "values": values_list}).astype(
            {"index": "int64", "values": "float64"}
        )

        return df

    @pytest.fixture
    def csv_float_expect(self):
        """Binary csv based on DataFrame with float values"""
        return (
            b"1640995215379000000,-0.2\n"
            b"1640995219176000000,-0.1\n"
            b"1640995227270000000,0.2\n"
            b"1640995267223000000,0.1\n"
            b"1640995271472000000,1.2\n"
        )

    def test__df_to_blob(self, monkeypatch, df_float, csv_float_expect):
        mock_response = Mock()

        def put_side_effect(*args, **kwargs):
            if data := kwargs.get("data"):
                assert data.read() == csv_float_expect
            return mock_response

        mock_put = Mock(side_effect=put_side_effect)

        monkeypatch.setattr(requests, "put", mock_put)

        blob_url = "http://foo/bar/baz"
        _ = drio.storage.storage._df_to_blob(df_float, blob_url)

        mock_put.assert_called_once_with(
            blob_url,
            headers={"x-ms-blob-type": "BlockBlob"},
            data=ANY,
        )

        mock_response.raise_for_status.assert_called_once()

    def test__df_to_blob2(self, df_float):

        blob_url = "http://example/blob/url"
        _ = drio.storage.storage._df_to_blob(df_float, blob_url)
