from pathlib import Path
from unittest.mock import ANY

import pandas as pd
import pytest
import requests
from requests import HTTPError

import datareservoirio as drio
from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


class Test__blob_to_df:
    """
    Tests the :func:`_blob_to_df` function.
    """

    @pytest.mark.parametrize(
        "blob_url, path_csv",
        [
            (
                "http://blob/dayfile/numeric",
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL/dayfile_numeric.csv",
            ),
            (
                "http://blob/dayfile/string",
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL/dayfile_string.csv",
            ),
        ],
    )
    def test__blob_to_df(self, blob_url, path_csv):
        """Tests ``_blob_to_df`` function."""
        df_out = drio.storage.storage._blob_to_df(blob_url)

        df_expect = DataHandler.from_csv(path_csv).as_dataframe()

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_raise_for_status(self):
        """Tests if ``raise_for_status`` is called"""
        with pytest.raises(requests.HTTPError):
            blob_url = "http://example/no/exist"
            _ = drio.storage.storage._blob_to_df(blob_url)


class Test__df_to_blob:
    """
    Tests the :func:`_df_to_blob` function.
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
    def test__df_to_blob(self, data, request):
        data = request.getfixturevalue(data)
        blob_url = "http://example/blob/url"
        _ = drio.storage.storage._df_to_blob(data.as_dataframe(), blob_url)

    @pytest.mark.parametrize("data", ("data_float", "data_string"))
    def test__df_to_blob_raises_series(self, data, request):
        data = request.getfixturevalue(data)
        with pytest.raises(ValueError):
            blob_url = "http://example/blob/url"
            _ = drio.storage.storage._df_to_blob(data.as_series(), blob_url)

    @pytest.mark.parametrize("data", ("data_float", "data_string"))
    def test__df_to_blob_call_args_2(
        self, mock_requests, bytesio_with_memory, data, request
    ):
        data = request.getfixturevalue(data)
        blob_url = "http://example/blob/url"
        _ = drio.storage.storage._df_to_blob(data.as_dataframe(), blob_url)

        mock_requests.assert_called_once_with(
            method="put",
            url=blob_url,
            headers={"x-ms-blob-type": "BlockBlob"},
            data=ANY,
        )

        assert mock_requests.call_args.kwargs["data"].memory == data.as_binary_csv()


class Test_Storage:
    """
    Tests the ``datareservoirio.storage.Storage`` class.
    """

    @pytest.fixture
    def storage_no_cache(self, auth_session):
        return drio.storage.Storage(auth_session, cache=False, cache_opt=None)

    def test__init__(self, auth_session):
        storage = drio.storage.Storage(auth_session, cache=False, cache_opt=None)

        assert storage._storage_cache is None
        assert storage._session is auth_session

    def test__init__cache(self, auth_session):
        storage = drio.storage.Storage(
            auth_session,
            cache=True,
            cache_opt={"max_size": 1024, "cache_root": ".cache"},
        )

        assert isinstance(storage._storage_cache, drio.storage.StorageCache)
        assert storage._session is auth_session

    def test_get(self, storage_no_cache):

        blob_sequence = (
            {
                "Path": "1b0d906b34ce40d69520e46f49a54545/2022/12/30/day/csv/19356.csv",
                "Endpoint": "https://permanentprodu000p169.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2022/12/30/day/csv/19356.csv?versionid=2023-03-14T14:56:10.8583280Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T13%3A50%3A58Z&ske=2023-03-15T13%3A50%3A57Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=QTYi%2FAeiMFg72EyxC8d%2BV0M0lmgbYek%2BfGXhAXvme1U%3D",
                "ContentMd5": "reFOcOgW3qct5b0VJ/5g7g==",
            },
            {
                "Path": "1b0d906b34ce40d69520e46f49a54545/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https://permanentprodu003p208.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2022/12/31/day/csv/19357.csv?versionid=2023-03-14T14:56:11.0377879Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T14%3A00%3A24Z&ske=2023-03-15T14%3A00%3A24Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=MIchFxo2gfRsa82kqTgHtq1DRY7cQldsZ0jQi4ySPZE%3D",
                "ContentMd5": "7ZeXRmZnw057D3QxWIYWmQ==",
            },
            {
                "Path": "1b0d906b34ce40d69520e46f49a54545/2023/01/01/day/csv/19358.csv",
                "Endpoint": "https://permanentprodu003p153.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2023/01/01/day/csv/19358.csv?versionid=2023-03-14T14:56:11.1047474Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T13%3A44%3A11Z&ske=2023-03-15T13%3A43%3A49Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=Uo5EqCiYRXcWDjifsiom9uGi7CNhFkGZQ4lBsGgEmC8%3D",
                "ContentMd5": "L96VxzU07mh5lcEsbuul9g==",
            },
            {
                "Path": "1b0d906b34ce40d69520e46f49a54545/2023/01/02/day/csv/19359.csv",
                "Endpoint": "https://permanentprodu002p192.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2023/01/02/day/csv/19359.csv?versionid=2023-03-14T14:56:11.1906071Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T13%3A50%3A18Z&ske=2023-03-15T13%3A50%3A18Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=bto08GDGsK7M%2FZLXOQ%2Bhm3sgYd%2B23g6rs5fI0nCq9AQ%3D",
                "ContentMd5": "CYS6gLUWLhZO6fFcGdbItg==",
            }
        )

        df_out = storage_no_cache.get(blob_sequence)

        df_expect = DataHandler.from_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
        ).as_dataframe()

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_overlapping(self, storage_no_cache):
        target_url = "https://reservoir-api.4subsea.net/api/timeseries/693cb0b2-3599-46d3-b263-ea913a648535/data/days?start=1672358400000000000&end=1672617600000000000"
        df_out = storage_no_cache.get(target_url)

        df_expect = DataHandler.from_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "dataframe.csv",
        ).as_dataframe()

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_empty(self, storage_no_cache):
        blob_sequence = []
        df_out = storage_no_cache.get(blob_sequence)

        df_expect = pd.DataFrame(columns=("index", "values")).astype({"index": "int64"})

        pd.testing.assert_frame_equal(df_out, df_expect)
