import os
import shutil
import time
from pathlib import Path
from unittest.mock import ANY, call

import pandas as pd
import pytest
import requests
from requests import HTTPError

import datareservoirio as drio
from datareservoirio._utils import DataHandler
from datareservoirio.storage import StorageCache

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
            (
                "http://blob/dayfile/string/malformatted",
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL/dayfile_string_malformatted.csv",
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

    @pytest.fixture
    def storage_with_cache(self, monkeypatch, auth_session, tmp_path):
        # Cache all files by setting CACHE_THRESHOLD=0
        monkeypatch.setattr(drio.storage.StorageCache, "CACHE_THRESHOLD", 0)
        cache_opt = {"cache_root": tmp_path / ".cache", "max_size": 1024}
        return drio.storage.Storage(auth_session, cache=True, cache_opt=cache_opt)

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
            },
        )

        df_out = storage_no_cache.get(blob_sequence)

        df_expect = DataHandler.from_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
        ).as_dataframe()

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_overlapping(self, storage_no_cache):
        blob_sequence = (
            {
                "Path": "03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv",
                "Endpoint": "https: //permanentprodu000p106.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:17:44.5067517Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=csFUPlbzexTJkgrLszdJrKTum5jUi%2BWv2PnIN9yM92Y%3D",
                "ContentMd5": "fJ85MDJqsTW6zDJbd+Fa4A==",
            },
            {
                "Path": "03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https: //permanentprodu001p067.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:17:44.6722101Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A51%3A15Z&ske=2023-04-14T15%3A51%3A15Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=TMfeXQYlcAe%2BdZGSGy5Z1WTytf41uIUQlQKBlDOQ3b4%3D",
                "ContentMd5": "wXZFUzjC6SIs09OqkttZWQ==",
            },
            {
                "Path": "629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv",
                "Endpoint": "https: //permanentprodu002p193.blob.core.windows.net/data/629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:18:26.1211914Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A55%3A52Z&ske=2023-04-14T15%3A55%3A51Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=dwqY3aiVKRb6MEwQYw%2B34y4LJcp0VHLat1BBNl9sUX8%3D",
                "ContentMd5": "JQAxeHMZ69WSsEuanKMJHA==",
            },
            {
                "Path": "629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https: //permanentprodu001p232.blob.core.windows.net/data/629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:18:26.2782276Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A53%3A09Z&ske=2023-04-14T15%3A53%3A09Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=DmRXb%2F7p%2B%2BYp%2FcPvJV5jTUzLJGgsjfEyA6PL8Kv4LTo%3D",
                "ContentMd5": "n5FdtLw0noj575zc0gilog==",
            },
            {
                "Path": "1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https: //permanentprodu002p003.blob.core.windows.net/data/1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:19:41.2836525Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A57%3A15Z&ske=2023-04-14T15%3A57%3A15Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=7tMAHyWBldWmECe3fyb%2B9D8RcN9xKNk%2FIJva%2B5vkpW0%3D",
                "ContentMd5": "c4cRzdbJCUkYa1JkprsUWw==",
            },
            {
                "Path": "1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv",
                "Endpoint": "https: //permanentprodu002p058.blob.core.windows.net/data/1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv?versionid=2023-04-14T13:19:41.5175166Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=YRmhRwUe0Fw40bj2Jh2XMFFtsAKNE6E5FVqK4rbIGhg%3D",
                "ContentMd5": "6BmmWa7uXis3+xZdR2tVwg==",
            },
        )

        df_out = storage_no_cache.get(blob_sequence)

        df_expect = DataHandler.from_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "dataframe.csv",
        ).as_dataframe()

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_empty(self, storage_no_cache):
        blob_sequence = []
        df_out = storage_no_cache.get(blob_sequence)

        df_expect = pd.DataFrame(columns=("index", "values")).astype({"index": "int64"})

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_with_cache(self, storage_with_cache, tmp_path):
        STOREFORMATVERSION = "v3"
        CACHE_PATH = tmp_path / ".cache" / STOREFORMATVERSION

        # Check that the cache folder is made, and that it is empty
        assert CACHE_PATH.exists()
        assert len(list(CACHE_PATH.iterdir())) == 0

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
            },
        )

        df_expect = DataHandler.from_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
        ).as_dataframe()

        # Get from remote storage (and cache the data)
        df_out = storage_with_cache.get(blob_sequence)

        pd.testing.assert_frame_equal(df_out, df_expect)

        # Check that the cache folder now contains four files
        assert len(list(CACHE_PATH.iterdir())) == 4

        # Get from cache (and check that the data actually is read from cache files)
        time_before_get = time.time()
        time.sleep(0.1)
        df_out = storage_with_cache.get(blob_sequence)
        time.sleep(0.1)
        time_after_get = time.time()
        for cache_file_i in CACHE_PATH.iterdir():
            time_access_file_i = os.path.getatime(cache_file_i)  # last access time
            assert time_before_get < time_access_file_i < time_after_get

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_overlapping_with_cache(self, storage_with_cache, tmp_path):
        STOREFORMATVERSION = "v3"
        CACHE_PATH = tmp_path / ".cache" / STOREFORMATVERSION

        # Check that the cache folder is made, and that it is empty
        assert CACHE_PATH.exists()
        assert len(list(CACHE_PATH.iterdir())) == 0

        blob_sequence = (
            {
                "Path": "03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv",
                "Endpoint": "https: //permanentprodu000p106.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:17:44.5067517Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=csFUPlbzexTJkgrLszdJrKTum5jUi%2BWv2PnIN9yM92Y%3D",
                "ContentMd5": "fJ85MDJqsTW6zDJbd+Fa4A==",
            },
            {
                "Path": "03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https: //permanentprodu001p067.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:17:44.6722101Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A51%3A15Z&ske=2023-04-14T15%3A51%3A15Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=TMfeXQYlcAe%2BdZGSGy5Z1WTytf41uIUQlQKBlDOQ3b4%3D",
                "ContentMd5": "wXZFUzjC6SIs09OqkttZWQ==",
            },
            {
                "Path": "629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv",
                "Endpoint": "https: //permanentprodu002p193.blob.core.windows.net/data/629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:18:26.1211914Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A55%3A52Z&ske=2023-04-14T15%3A55%3A51Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=dwqY3aiVKRb6MEwQYw%2B34y4LJcp0VHLat1BBNl9sUX8%3D",
                "ContentMd5": "JQAxeHMZ69WSsEuanKMJHA==",
            },
            {
                "Path": "629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https: //permanentprodu001p232.blob.core.windows.net/data/629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:18:26.2782276Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A53%3A09Z&ske=2023-04-14T15%3A53%3A09Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=DmRXb%2F7p%2B%2BYp%2FcPvJV5jTUzLJGgsjfEyA6PL8Kv4LTo%3D",
                "ContentMd5": "n5FdtLw0noj575zc0gilog==",
            },
            {
                "Path": "1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv",
                "Endpoint": "https: //permanentprodu002p003.blob.core.windows.net/data/1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:19:41.2836525Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A57%3A15Z&ske=2023-04-14T15%3A57%3A15Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=7tMAHyWBldWmECe3fyb%2B9D8RcN9xKNk%2FIJva%2B5vkpW0%3D",
                "ContentMd5": "c4cRzdbJCUkYa1JkprsUWw==",
            },
            {
                "Path": "1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv",
                "Endpoint": "https: //permanentprodu002p058.blob.core.windows.net/data/1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv?versionid=2023-04-14T13:19:41.5175166Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=YRmhRwUe0Fw40bj2Jh2XMFFtsAKNE6E5FVqK4rbIGhg%3D",
                "ContentMd5": "6BmmWa7uXis3+xZdR2tVwg==",
            },
        )

        df_expect = DataHandler.from_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "dataframe.csv",
        ).as_dataframe()

        # Get from remote storage (and cache the data)
        df_out = storage_with_cache.get(blob_sequence)

        pd.testing.assert_frame_equal(df_out, df_expect)

        # Check that the cache folder now contains four files
        assert len(list(CACHE_PATH.iterdir())) == 6

        # Get from cache (and check that the data actually is read from cache files)
        time_before_get = time.time()
        time.sleep(0.1)
        df_out = storage_with_cache.get(blob_sequence)
        time.sleep(0.1)
        time_after_get = time.time()
        for cache_file_i in CACHE_PATH.iterdir():
            time_access_file_i = os.path.getatime(cache_file_i)  # last access time
            assert time_before_get < time_access_file_i < time_after_get

        pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_empty_with_cache(self, storage_with_cache, tmp_path):
        """
        Empty data will not be cached since the number of rows is below the CACHE_THRESHOLD
        """

        STOREFORMATVERSION = "v3"
        CACHE_PATH = tmp_path / ".cache" / STOREFORMATVERSION

        # Check that the cache folder is made, and that it is empty
        assert CACHE_PATH.exists()
        assert len(list(CACHE_PATH.iterdir())) == 0

        blob_sequence = []

        df_expect = pd.DataFrame(columns=("index", "values")).astype({"index": "int64"})

        # Get from remote storage
        df_out = storage_with_cache.get(blob_sequence)

        pd.testing.assert_frame_equal(df_out, df_expect)

        # Check that the cache folder still contains zero files
        # (since the number of rows is below the CACHE_THRESHOLD)
        assert len(list(CACHE_PATH.iterdir())) == 0

        # Get data again (still from remote storage, since no cache files are made)
        df_out = storage_with_cache.get(blob_sequence)

        pd.testing.assert_frame_equal(df_out, df_expect)

    @pytest.mark.parametrize(
        "chunk, file_path",
        [
            (
                {
                    "Path": "foo/bar/baz",
                    "Endpoint": "http://blob/dayfile/numeric",
                    "ContentMd5": "1234abc",
                },
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL"
                / "dayfile_numeric.csv",
            ),
            (
                {
                    "Path": "foo/bar/baz",
                    "Endpoint": "http://blob/dayfile/string",
                    "ContentMd5": "1234abc",
                },
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL"
                / "dayfile_string.csv",
            ),
            (
                {
                    "Path": "foo/bar/baz",
                    "Endpoint": "http://blob/dayfile/string/malformatted",
                    "ContentMd5": "1234abc",
                },
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL"
                / "dayfile_string_malformatted.csv",
            ),
        ],
    )
    def test__blob_to_df(self, storage_no_cache, chunk, file_path):
        df_out = storage_no_cache._blob_to_df(chunk)
        df_expect = DataHandler.from_csv(file_path).as_dataframe()
        pd.testing.assert_frame_equal(df_out, df_expect)

    @pytest.mark.parametrize(
        "chunk, file_path",
        [
            (
                {
                    "Path": "foo/bar/baz",
                    "Endpoint": "http://blob/dayfile/numeric",
                    "ContentMd5": "1234abc",
                },
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL"
                / "dayfile_numeric.csv",
            ),
            (
                {
                    "Path": "foo/bar/baz",
                    "Endpoint": "http://blob/dayfile/string",
                    "ContentMd5": "1234abc",
                },
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL"
                / "dayfile_string.csv",
            ),
            (
                {
                    "Path": "foo/bar/baz",
                    "Endpoint": "http://blob/dayfile/string/malformatted",
                    "ContentMd5": "1234abc",
                },
                TEST_PATH.parent
                / "testdata"
                / "RESPONSE_CASES_GENERAL"
                / "dayfile_string_malformatted.csv",
            ),
        ],
    )
    def test__blob_to_df_with_cache(
        self, tmp_path, storage_with_cache, chunk, file_path
    ):
        STOREFORMATVERSION = "v3"
        CACHE_PATH = tmp_path / ".cache" / STOREFORMATVERSION

        # Check that the cache folder is made, and that it is empty
        assert CACHE_PATH.exists()
        assert len(list(CACHE_PATH.iterdir())) == 0

        df_out = storage_with_cache._blob_to_df(chunk)
        df_expect = DataHandler.from_csv(file_path).as_dataframe()
        pd.testing.assert_frame_equal(df_out, df_expect)

        # Check that the cache folder now contains one file
        assert len(list(CACHE_PATH.iterdir())) == 1

    @pytest.mark.parametrize("data", ("data_float", "data_string"))
    def test_put(
        self, request, mock_requests, bytesio_with_memory, storage_no_cache, data
    ):
        data = request.getfixturevalue(data)

        df = data.as_dataframe()

        storage_no_cache.put(
            df,
            "http://example/blob/url",
            (
                "POST",
                "https://reservoir-api.4subsea.net/api/files/commit",
                {"json": {"FileId": "1234"}},
            ),
        )

        calls_expected = [
            call(
                method="put",
                url="http://example/blob/url",
                headers={"x-ms-blob-type": "BlockBlob"},
                data=ANY,
            ),
            call(
                method="POST",
                url="https://reservoir-api.4subsea.net/api/files/commit",
                json={"FileId": "1234"},
            ),
        ]

        assert (
            mock_requests.call_args_list[0].kwargs["data"].memory
            == data.as_binary_csv()
        )

        mock_requests.assert_has_calls(calls_expected)

    def test_put_raise_for_status(self, storage_no_cache, data_float):
        df = data_float.as_dataframe()

        with pytest.raises(HTTPError):
            storage_no_cache.put(
                df,
                "http://example/put/raises",
                (
                    "POST",
                    "https://reservoir-api.4subsea.net/api/files/commit",
                    {"json": {"FileId": "1234"}},
                ),
            )

    def test_put_raise_for_status2(self, storage_no_cache, data_float):
        df = data_float.as_dataframe()

        with pytest.raises(HTTPError):
            storage_no_cache.put(
                df,
                "http://example/blob/url",
                (
                    "POST",
                    "http://example/post/raises",
                    {"json": {"FileId": "1234"}},
                ),
            )


class Test_StorageCache:
    @pytest.fixture
    def storage_cache_empty(self, tmp_path):
        """StorageCache instance with empty cache"""
        storage_cache = StorageCache(
            max_size=1024,
            cache_root=tmp_path / ".cache",
        )
        return storage_cache

    @pytest.fixture
    def tmp_root_with_data(self, tmp_path, STOREFORMATVERSION):
        """Temporary cache root (with data)"""
        dst_cache_root = tmp_path
        dst_cache_path = dst_cache_root / STOREFORMATVERSION
        dst_cache_path.mkdir()
        src_cache_path = (
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "cache" / "v3"
        )
        for src_cache_file_i in src_cache_path.iterdir():
            shutil.copyfile(src_cache_file_i, dst_cache_path / src_cache_file_i.name)
        return dst_cache_root

    @pytest.fixture
    def storage_cache(self, tmp_root_with_data):
        """StorageCache instance with data"""
        storage_cache = StorageCache(
            max_size=1024,
            cache_root=tmp_root_with_data,
        )
        return storage_cache

    @pytest.fixture
    def chunk(self):
        chunk = {
            "Account": "permanentprodu000p106",
            "SasKey": "skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=csFUPlbzexTJkgrLszdJrKTum5jUi%2BWv2PnIN9yM92Y%3D",
            "SasKeyExpirationTime": "2023-04-14T15: 27: 42.3326841+00: 00",
            "Container": "data",
            "Path": "03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv",
            "Endpoint": "https: //permanentprodu000p106.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:17:44.5067517Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=csFUPlbzexTJkgrLszdJrKTum5jUi%2BWv2PnIN9yM92Y%3D",
            "ContentMd5": "fJ85MDJqsTW6zDJbd+Fa4A==",
            "VersionId": "2023-04-14T13: 17: 44.5067517Z",
            "DaysSinceEpoch": 19356,
        }
        return chunk

    def test__init__(self):
        storage_cache = StorageCache(
            max_size=1024, cache_root=None, cache_folder="datareservoirio"
        )

        assert storage_cache._max_size == 1024 * 1024**2

    def test__init__cache_root(self, tmp_path, STOREFORMATVERSION):
        assert not (tmp_path / ".cache_" / STOREFORMATVERSION).exists()
        StorageCache(cache_root=tmp_path / ".cache_")
        assert (tmp_path / ".cache_" / STOREFORMATVERSION).exists()

    def test__init_cache_dir(self, storage_cache_empty, tmp_path, STOREFORMATVERSION):
        assert not (tmp_path / ".cache_" / STOREFORMATVERSION).exists()

        storage_cache_empty._init_cache_dir(tmp_path / ".cache_", "datareservoirio")

        root_expect = tmp_path / ".cache_"
        assert storage_cache_empty._root == str(root_expect)
        assert (root_expect / STOREFORMATVERSION).exists()

    def test__init_cache_dir_exists(
        self, storage_cache_empty, tmp_path, STOREFORMATVERSION
    ):
        os.makedirs(tmp_path / ".cache_" / STOREFORMATVERSION)
        assert (tmp_path / ".cache_" / STOREFORMATVERSION).exists()

        storage_cache_empty._init_cache_dir(tmp_path / ".cache_", "datareservoirio")

        root_expect = tmp_path / ".cache_"
        assert storage_cache_empty._root == str(root_expect)
        assert (root_expect / STOREFORMATVERSION).exists()

    def test__init_cache_dir_default(self, storage_cache_empty, STOREFORMATVERSION):
        storage_cache_empty._init_cache_dir(None, "datareservoirio")

        root_expect = drio.appdirs.user_cache_dir("datareservoirio")
        cache_path_expect = os.path.join(root_expect, STOREFORMATVERSION)
        assert storage_cache_empty._root == str(root_expect)
        assert os.path.exists(cache_path_expect)

    def test__cache_hive(self, storage_cache_empty, STOREFORMATVERSION):
        assert storage_cache_empty._cache_hive == STOREFORMATVERSION

    def test_cache_root(self, storage_cache_empty, tmp_path):
        assert storage_cache_empty.cache_root == str(tmp_path / ".cache")

    def test__cache_path(self, storage_cache_empty, tmp_path, STOREFORMATVERSION):
        cache_path_expect = str(tmp_path / ".cache" / STOREFORMATVERSION)
        assert storage_cache_empty._cache_path == cache_path_expect

    def test_reset_cache(self, storage_cache, tmp_root_with_data):
        assert len(list(tmp_root_with_data.iterdir())) != 0

        storage_cache.reset_cache()

        assert tmp_root_with_data.exists()
        assert len(list(tmp_root_with_data.iterdir())) == 0

    def test_get(self, storage_cache, chunk):
        data_out = storage_cache.get(chunk)

        data_expect_path = (
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "19356.csv"
        )
        data_expect = DataHandler.from_csv(data_expect_path).as_dataframe()

        pd.testing.assert_frame_equal(data_out, data_expect)

    def test_get_empty(self, storage_cache_empty, chunk):
        data_out = storage_cache_empty.get(chunk)
        assert data_out is None

    def test__get_cache_id_md5(self, storage_cache_empty, chunk):
        id_out, md5_out = storage_cache_empty._get_cache_id_md5(chunk)

        id_expect = "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv"
        md5_expect = "Zko4NU1ESnFzVFc2ekRKYmQrRmE0QT09"

        assert id_out == id_expect
        assert md5_out == md5_expect

    def test_put(self, storage_cache_empty, chunk):
        data_path = TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "19356.csv"
        data = DataHandler.from_csv(data_path).as_dataframe()

        storage_cache_empty.put(data, chunk)

        n_files_cached = len(os.listdir(storage_cache_empty._cache_path))
        assert n_files_cached == 1
        assert storage_cache_empty._cache_index.exists(
            "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv",
            "Zko4NU1ESnFzVFc2ekRKYmQrRmE0QT09",
        )

    def test_put_tiny(self, storage_cache_empty, chunk, data_float):
        data_tiny = data_float.as_dataframe()  # tiny file
        storage_cache_empty.put(data_tiny, chunk)

        n_files_cached = len(os.listdir(storage_cache_empty._cache_path))
        assert n_files_cached == 0   # tiny files are not cached
