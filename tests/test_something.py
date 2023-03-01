from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

import datareservoirio as drio


TEST_PATH = Path(__file__).parent


def test_something():
    """
    Temporary test so that tox runs without errors while waiting for actual tests
    to be added.

    TODO: Remove when actual tests are added.
    """
    assert 1 == 1


class Test__blob_to_df:
    @pytest.fixture
    def mock_response_get(self, monkeypatch, get_response):

        def mock_get(*args, **kwargs):
            content_path = TEST_PATH / "testdata" / "example_drio_blob_file.csv"
            get_response._content_path = content_path
            return get_response

        monkeypatch.setattr(drio.storage.storage.requests, "get", mock_get)

    @pytest.fixture
    def df_expect(self):
        df = pd.read_csv(
            TEST_PATH / "testdata" / "example_drio_blob_file.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "str"},
            encoding="utf-8",
        ).astype({"values": "float64"}, errors="ignore")

        return df

    def test__blob_to_df(self, mock_response_get, df_expect):
        df_out = drio.storage.storage._blob_to_df("some/url")
        pd.testing.assert_frame_equal(df_out, df_expect)
