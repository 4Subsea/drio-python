from pathlib import Path

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
