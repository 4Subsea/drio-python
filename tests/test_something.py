import json
from pathlib import Path

import pandas as pd
import pytest
import requests

import datareservoirio as drio

TEST_PATH = Path(__file__).parent


def test_something():
    """
    Temporary test so that tox runs without errors while waiting for actual tests
    to be added.

    TODO: Remove when actual tests are added.
    """
    assert 1 == 1


def test_another_thing(mock_requests_get):
    df_out = drio.storage.storage._blob_to_df("example/drio/blob/file.csv")

    df_expect = pd.read_csv(
        TEST_PATH / "testdata" / "example_drio_blob_file.csv",
        header=None,
        names=("index", "values"),
        dtype={"index": "int64", "values": "str"},
        encoding="utf-8",
    ).astype({"values": "float64"}, errors="ignore")

    pd.testing.assert_frame_equal(df_out, df_expect)


def test_one_more_thing(mock_requests_get):
    response = drio.storage.storage.requests.get("example/drio/api/output.json")
    dict_out = response.json()

    with open(TEST_PATH / "testdata" / "example_drio_api_output.json", mode="r") as f:
        dict_expect = json.load(f)

    assert dict_out == dict_expect


def test_yet_another_thing(mock_requests_get):
    response = drio.storage.storage.requests.get("example/drio/api/output")
    dict_out = response.json()

    with open(TEST_PATH / "testdata" / "example_drio_api_output.json", mode="r") as f:
        dict_expect = json.load(f)

    assert dict_out == dict_expect
