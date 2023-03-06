from hashlib import md5
from pathlib import Path
from unittest.mock import ANY, Mock, patch

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

    def test__blob_to_df(self, mock_requests_get):
        """Tests ``_blob_to_df`` function."""
        blob_url = "example/drio/blob/file"
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

    def test_raise_for_status(self, mock_requests_get):
        """Tests if ``raise_for_status`` is called"""
        with pytest.raises(requests.HTTPError):
            blob_url = "example/no/exist"
            _ = drio.storage.storage._blob_to_df(blob_url)


class Test__df_to_blob:
    """
    Tests the :func:`_df_to_blob` function.
    """

    def test__df_to_blob(self, tmp_path):
        """Tests ``_df_to_blob`` function."""

        csv_file = TEST_PATH.parent / "testdata" / "example_drio_blob_file.csv"
        df = pd.read_csv(
            csv_file,
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "str"},
            encoding="utf-8",
        ).astype({"values": "float64"}, errors="ignore")

        mock_response = Mock()

        with patch.object(
            requests, "put", side_effect=lambda *args, **kwargs: mock_response
        ) as mock_put:
            blob_url = "example/blob/endpoint"
            _ = drio.storage.storage._df_to_blob(df, blob_url)

            mock_put.assert_called_once_with(
                blob_url, headers={"x-ms-blob-type": "BlockBlob"}, data=ANY
            )
            mock_response.raise_for_status.assert_called_once()
