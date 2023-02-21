import io
import os
import unittest
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, Mock, call, patch

import numpy as np
import pandas as pd
import pytest
import requests

from datareservoirio.appdirs import user_cache_dir
from datareservoirio.storage import Storage, StorageCache
from datareservoirio.storage.storage import (
    _blob_to_df,
    _df_to_blob,
    _encode_for_path_safety,
)

TEST_PATH = Path(__file__).parent


@pytest.fixture
def mock_requests_get(monkeypatch):
    class MockResponse:
        """
        Used to mock the response from ``requests.get``, based on data in a local
        file.

        Needed to be able to test the ``_blob_to_df`` function.

        Parameters
        ----------
        path : str
            File path.
        """

        def __init__(self, path):
            self._path = path

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=1):
            with open(self._path, mode="rb") as f:
                while content_i := f.read(chunk_size):
                    yield content_i

    def mock_get(url, *args, **kwargs):
        return MockResponse(url)

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def numeric_blob_file_path():
    return TEST_PATH / "testdata" / "numeric_blob.csv"


@pytest.fixture
def numeric_blob_df():
    df = pd.DataFrame(
        {
            "index": [
                1624838400000000000,
                1624838400097656250,
                1624838400195312500,
                1624838400292968750,
                1624838400390625000,
                1624838400488281250,
                1624838400585937500,
                1624838400683593750,
                1624838400781250000,
                1624838400878906250,
            ],
            "values": [
                -0.200514,
                -0.200514,
                -0.203507,
                -0.202312,
                -0.202311,
                -0.188543,
                -0.190938,
                -0.193332,
                -0.192135,
                -0.186149,
            ],
        },
    )
    return df


@pytest.fixture
def str_blob_file_path():
    return TEST_PATH / "testdata" / "str_blob.csv"


@pytest.fixture
def str_blob_df():
    df = pd.DataFrame(
        {
            "index": [
                1624838400000000000,
                1624838400097656250,
                1624838400195312500,
                1624838400292968750,
                1624838400390625000,
                1624838400488281250,
                1624838400585937500,
                1624838400683593750,
                1624838400781250000,
                1624838400878906250,
            ],
            "values": [
                "Aa",
                "Bb",
                "Cc",
                "Dd",
                "Ee",
                "Ff",
                "Gg",
                "Hh",
                "Ii",
                "Jj",
            ],
        },
    )
    return df


@pytest.fixture()
def bytesio_with_memory():
    class BytesIOmemory(io.BytesIO):
        def close(self, *args, **kwargs):
            self.memory = self.getvalue()
            super().close(*args, **kwargs)

    return BytesIOmemory


class Test_Storage(unittest.TestCase):
    def setUp(self):
        self._timeseries_api = Mock()
        self.session = MagicMock()

        self.tid = "abc-123-xyz"

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "Files": [
                {
                    "Index": 0,
                    "FileId": "61745116-e25d-4e9c-b7ea-5d7ae4ab004b",
                    "Chunks": [
                        {
                            "Account": "permanentprodu003p144",
                            "SasKey": "skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-02-21T08%3A39%3A13Z&ske=2023-02-22T08%3A39%3A13Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-02-21T14%3A34%3A50Z&sr=b&sp=r&sig=3uHDX4JkocmafBv3ZslIR8xkv4x0q2zaNjCcWfw71eI%3D",
                            "SasKeyExpirationTime": "2023-02-21T14:34:50.8860219+00:00",
                            "Container": "data",
                            "Path": "61745116e25d4e9cb7ea5d7ae4ab004b/2021/06/28/day/csv/18806.csv",
                            "Endpoint": "https:://go-here-for-blob.com/segment_0",
                            "ContentMd5": "1J+DJHBbx2Kgq9S8WnsV3A==",
                            "VersionId": "2021-08-31T13:53:27.3874185Z",
                            "DaysSinceEpoch": 18806,
                        }
                    ],
                }
            ]
        }
        self.session.request.return_value = mock_response

        self.storage = Storage(session=self.session, cache=False)

    @patch("datareservoirio.storage.storage._blob_to_df")
    def test_get(self, mock_remote_get):
        data_remote = pd.DataFrame({"index": [1, 2, 3, 4], "values": [1, 2, 3, 4]})
        mock_remote_get.return_value = data_remote.copy()  # Inplace manipulation occurs

        data_out = self.storage.get("https://myapi/data/days?start=1&end=2")

        mock_remote_get.assert_called_once_with(
            "https:://go-here-for-blob.com/segment_0"
        )
        pd.testing.assert_frame_equal(data_out, data_remote)

    def test_put(self):
        target_url = "https://remote-storage.com/myblob"
        commit_request = ("POST", "https://api/files/commit", {"json": {"FileId": 42}})

        df_expected_sent = pd.DataFrame({"index": [1, 2, 3, 4], "values": [1, 2, 3, 4]})

        with patch("datareservoirio.storage.storage._df_to_blob") as uploader:
            self.storage.put(df_expected_sent, target_url, commit_request)

        (df_sent, target_url_sent), _ = uploader.call_args
        assert target_url == target_url_sent
        pd.testing.assert_frame_equal(df_expected_sent, df_sent)

        self.session.request.assert_called_once_with(
            *commit_request[:2], **commit_request[-1]
        )

    def test__days_response_url_sequence(self):
        response_json = {
            "Files": [
                {
                    "Index": 0,
                    "FileId": "61745116-e25d-4e9c-b7ea-5d7ae4ab004b",
                    "Chunks": [
                        {
                            "Account": "permanentprodu003p144",
                            "SasKey": "skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-02-21T08%3A39%3A13Z&ske=2023-02-22T08%3A39%3A13Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-02-21T14%3A34%3A50Z&sr=b&sp=r&sig=3uHDX4JkocmafBv3ZslIR8xkv4x0q2zaNjCcWfw71eI%3D",
                            "SasKeyExpirationTime": "2023-02-21T14:34:50.8860219+00:00",
                            "Container": "data",
                            "Path": "segment_0.csv",
                            "Endpoint": "https://go-here-for-blob.com/segment_0",
                            "ContentMd5": "md5_0",
                            "VersionId": "2021-08-31T13:53:27.3874185Z",
                            "DaysSinceEpoch": 18806,
                        },
                        {
                            "Account": "permanentprodu003p144",
                            "SasKey": "skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-02-21T08%3A39%3A13Z&ske=2023-02-22T08%3A39%3A13Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-02-21T14%3A34%3A50Z&sr=b&sp=r&sig=3uHDX4JkocmafBv3ZslIR8xkv4x0q2zaNjCcWfw71eI%3D",
                            "SasKeyExpirationTime": "2023-02-21T14:34:50.8860219+00:00",
                            "Container": "data",
                            "Path": "segment_1.csv",
                            "Endpoint": "https://go-here-for-blob.com/segment_1",
                            "ContentMd5": "md5_1",
                            "VersionId": "2021-08-31T13:53:27.3874185Z",
                            "DaysSinceEpoch": 18807,
                        },
                    ],
                },
                {
                    "Index": 1,
                    "FileId": "51745116-e25d-4e9c-b7ea-5d7ae4ab004b",
                    "Chunks": [
                        {
                            "Account": "permanentprodu003p144",
                            "SasKey": "skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-02-21T08%3A39%3A13Z&ske=2023-02-22T08%3A39%3A13Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-02-21T14%3A34%3A50Z&sr=b&sp=r&sig=3uHDX4JkocmafBv3ZslIR8xkv4x0q2zaNjCcWfw71eI%3D",
                            "SasKeyExpirationTime": "2023-02-21T14:34:50.8860219+00:00",
                            "Container": "data",
                            "Path": "segment_3.csv",
                            "Endpoint": "https://go-here-for-blob.com/segment_3",
                            "ContentMd5": "md5_3",
                            "VersionId": "2021-08-31T13:53:27.3874185Z",
                            "DaysSinceEpoch": 18807,
                        }
                    ],
                },
            ]
        }

        out_expected = [
            {
                "Endpoint": "https://go-here-for-blob.com/segment_0",
                "Path": "segment_0.csv",
                "ContentMd5": "md5_0",
            },
            {
                "Endpoint": "https://go-here-for-blob.com/segment_1",
                "Path": "segment_1.csv",
                "ContentMd5": "md5_1",
            },
            {
                "Endpoint": "https://go-here-for-blob.com/segment_3",
                "Path": "segment_3.csv",
                "ContentMd5": "md5_3",
            },
        ]

        out = self.storage._days_response_url_sequence(response_json)
        assert out == out_expected


# class Test_BaseDownloader(unittest.TestCase):
#     def test_init(self):
#         BaseDownloader()

#     @patch("datareservoirio.storage.storage._blob_to_df")
#     def test_get(self, mock_remote_get):
#         response = {
#             "Files": [
#                 {"Chunks": [{"Endpoint": "https:://go-here-for-blob.com/segment_0"}]},
#                 {"Chunks": [{"Endpoint": "https:://go-here-for-blob.com/segment_1"}]},
#                 {"Chunks": [{"Endpoint": "https:://go-here-for-blob.com/segment_2"}]},
#             ]
#         }

#         mock_remote_get.side_effect = [
#             pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]}),
#             pd.DataFrame({"index": [3, 4, 5], "values": [5.0, 4.0, 5.0]}),
#             pd.DataFrame({"index": [1, 5, 9], "values": [9.0, 8.0, 1.0]}),
#         ]

#         df_expected = pd.DataFrame(
#             {"index": [1, 2, 3, 4, 5, 9], "values": [9.0, 2.0, 5.0, 4.0, 8.0, 1.0]}
#         )

#         base_downloader = BaseDownloader()

#         df_out = base_downloader.get(response)

#         pd.testing.assert_frame_equal(df_expected, df_out)

#         calls = [
#             call("https:://go-here-for-blob.com/segment_0"),
#             call("https:://go-here-for-blob.com/segment_1"),
#             call("https:://go-here-for-blob.com/segment_2"),
#         ]
#         mock_remote_get.assert_has_calls(calls)

#     @patch("datareservoirio.storage.storage._blob_to_df")
#     def test_get_with_text_data(self, mock_remote_get):
#         response = {
#             "Files": [
#                 {"Chunks": [{"Endpoint": "https:://go-here-for-blob.com/segment_0"}]},
#                 {"Chunks": [{"Endpoint": "https:://go-here-for-blob.com/segment_1"}]},
#                 {"Chunks": [{"Endpoint": "https:://go-here-for-blob.com/segment_2"}]},
#             ]
#         }

#         mock_remote_get.side_effect = [
#             pd.DataFrame({"index": [1, 2, 3], "values": ["a", "b", "c"]}),
#             pd.DataFrame({"index": [3, 4, 5], "values": ["d", "e", "f"]}),
#             pd.DataFrame({"index": [1, 5, 9], "values": ["g", "h", "i"]}),
#         ]

#         df_expected = pd.DataFrame(
#             {"index": [1, 2, 3, 4, 5, 9], "values": ["g", "b", "d", "e", "h", "i"]}
#         )

#         base_downloader = BaseDownloader()

#         df_out = base_downloader.get(response)

#         pd.testing.assert_frame_equal(df_expected, df_out)

#         calls = [
#             call("https:://go-here-for-blob.com/segment_0"),
#             call("https:://go-here-for-blob.com/segment_1"),
#             call("https:://go-here-for-blob.com/segment_2"),
#         ]
#         mock_remote_get.assert_has_calls(calls)

#         # mock_backend = MagicMock()
#         # base_downloader = BaseDownloader(mock_backend)

#         # response = {
#         #     "Files": [{"Chunks": "abc1"}, {"Chunks": "abc2"}, {"Chunks": "abc3"}]
#         # }

#         # with patch.object(
#         #     base_downloader, "_download_chunks_as_dataframe"
#         # ) as mock_download:
#         #     mock_download.side_effect = [
#         #         pd.DataFrame({"index": [1, 2, 3], "values": ["a", "b", "c"]}).set_index(
#         #             "index"
#         #         ),
#         #         pd.DataFrame({"index": [3, 4, 5], "values": ["d", "e", "f"]}).set_index(
#         #             "index"
#         #         ),
#         #         pd.DataFrame({"index": [1, 5, 9], "values": ["g", "h", "i"]}).set_index(
#         #             "index"
#         #         ),
#         #     ]

#         #     df_expected = pd.DataFrame(
#         #         {"index": [1, 2, 3, 4, 5, 9], "values": ["g", "b", "d", "e", "h", "i"]}
#         #     )
#         #     df_out = base_downloader.get(response)

#         # pd.testing.assert_frame_equal(df_expected, df_out)

#     def test_get_empty(self):
#         mock_backend = MagicMock()
#         base_downloader = BaseDownloader(mock_backend)

#         response = {"Files": []}

#         df_out = base_downloader.get(response)

#         df_expected = pd.DataFrame(
#             pd.DataFrame(columns=("index", "values")).astype({"index": "int64"})
#         )

#         mock_backend.assert_not_called()
#         pd.testing.assert_frame_equal(df_expected, df_out)

#     def test__combine_first_no_overlap(self):
#         df1 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
#         df2 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[6, 7, 8, 9])
#         df_expected = df1.combine_first(df2)
#         df_out = BaseDownloader._combine_first(df1, df2)
#         pd.testing.assert_series_equal(df_expected, df_out)

#     def test__combine_first_no_overlap_reversed_order(self):
#         df2 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
#         df1 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[6, 7, 8, 9])
#         df_expected = df1.combine_first(df2)
#         df_out = BaseDownloader._combine_first(df1, df2)
#         pd.testing.assert_series_equal(df_expected, df_out)

#     def test__combine_first_exact_overlap(self):
#         df1 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
#         df2 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[0, 1, 2, 3])
#         df_expected = df1.combine_first(df2)
#         df_out = BaseDownloader._combine_first(df1, df2)
#         pd.testing.assert_series_equal(df_expected, df_out)

#     def test__combine_first_partial_overlap(self):
#         df1 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
#         df2 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[2, 3, 4, 5])
#         df_expected = df1.combine_first(df2)
#         df_out = BaseDownloader._combine_first(df1, df2)
#         pd.testing.assert_series_equal(df_expected, df_out)

#     @patch("datareservoirio.storage.storage._blob_to_df")
#     def test__download_verified_chunk(self, mock_remote_get):
#         chunk = {"Endpoint": "https:://go-here-for-blob.com/segment_0"}

#         mock_remote_get.side_effect = [
#             pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]})
#         ]

#         df_expected = pd.DataFrame(
#             {"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]}
#         ).set_index("index")

#         base_downloader = BaseDownloader()

#         df_out = base_downloader._download_verified_chunk(chunk)
#         mock_remote_get.assert_called_once_with(chunk["Endpoint"])

#         pd.testing.assert_frame_equal(df_expected, df_out)

#     @patch("datareservoirio.storage.storage._blob_to_df")
#     def test__download_verified_chunk_w_duplicates(self, mock_remote_get):
#         chunk = {"Endpoint": "https:://go-here-for-blob.com/segment_0"}

#         mock_remote_get.side_effect = [
#             pd.DataFrame({"index": [1, 2, 2, 3], "values": [1.0, 2.0, 2.0, 3.0]})
#         ]

#         df_expected = pd.DataFrame(
#             {"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]}
#         ).set_index("index")

#         base_downloader = BaseDownloader()

#         df_out = base_downloader._download_verified_chunk(chunk)
#         mock_remote_get.assert_called_once_with(chunk["Endpoint"])

#         pd.testing.assert_frame_equal(df_expected, df_out)

#     @patch("datareservoirio.storage.storage._blob_to_df")
#     def test__download_chunks_as_dataframe(self, mock_remote_get):
#         chunk = [{"Endpoint": "https:://go-here-for-blob.com/segment_0"}]
#         mock_remote_get.side_effect = [
#             pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]})
#         ]

#         df_expected = pd.DataFrame(
#             {
#                 "index": [1, 2, 3],
#                 "values": [1.0, 2.0, 3.0],
#             }
#         ).set_index("index")

#         base_downloader = BaseDownloader()
#         df_out = base_downloader._download_chunks_as_dataframe(chunk)

#         mock_remote_get.assert_called_once_with(
#             "https:://go-here-for-blob.com/segment_0"
#         )
#         pd.testing.assert_frame_equal(df_expected, df_out)

#     def test__download_chunks_as_dataframe_no_chunks(self):
#         mock_backend = MagicMock()

#         base_downloader = BaseDownloader(mock_backend)
#         df_out = base_downloader._download_chunks_as_dataframe([])

#         df_expected = (
#             pd.DataFrame(columns=("index", "values"))
#             .astype({"index": "int64"})
#             .set_index("index")
#         )

#         mock_backend.assert_not_called()
#         pd.testing.assert_frame_equal(df_expected, df_out)


class Test_StorageCache(unittest.TestCase):
    def setUp(self):
        makedirs_patcher = patch("datareservoirio.storage.storage.os.makedirs")
        self._makedirs_patch = makedirs_patcher.start()

        session_patcher = patch("requests.Session")
        self._session_patch = session_patcher.start()

        cacheio_patcher = patch("datareservoirio.storage.storage.CacheIO.__init__")
        self._cacheio_patch = cacheio_patcher.start()

        cacheindex_patcher = patch("datareservoirio.storage.storage._CacheIndex")
        self._cacheindex_patch = cacheindex_patcher.start()

        evict_patcher = patch("datareservoirio.storage.StorageCache._evict_from_cache")
        self._evict_patch = evict_patcher.start()

        self.addCleanup(patch.stopall)

    def test_init(self):
        StorageCache()
        self._evict_patch.assert_called_once()

    def test_cache_hive(self):
        cache = StorageCache()
        self.assertEqual(cache._cache_hive, cache.STOREFORMATVERSION)

    def test_cache_root(self):
        cache = StorageCache()
        self.assertEqual(
            cache.cache_root, os.path.abspath(user_cache_dir("datareservoirio"))
        )

        cache = StorageCache(cache_folder="sometingelse")
        self.assertEqual(
            cache.cache_root, os.path.abspath(user_cache_dir("sometingelse"))
        )

        cache = StorageCache(cache_root="home", cache_folder="anywhere")
        self.assertEqual(cache.cache_root, os.path.abspath("home"))

    def test_cache_path(self):
        cache = StorageCache(cache_root="home")
        self.assertEqual(
            os.path.join(os.path.abspath("home"), cache.STOREFORMATVERSION),
            cache._cache_path,
        )

    def test_reset_cache(self):
        cache = StorageCache()
        with patch.object(cache, "_evict_entry_root") as mock_evict:
            cache.reset_cache()
        mock_evict.assert_called_once_with(cache.cache_root)

    def test_get_cache_id_md5(self):
        chunk = {"Path": "abc-123/def_456", "ContentMd5": "md5-123"}

        cache = StorageCache()
        id_out, md5_out = cache._get_cache_id_md5(chunk)
        id_expected = cache._cache_format + "abc123def456"
        md5_expected = _encode_for_path_safety("md5-123")
        self.assertEqual(id_out, id_expected)
        self.assertEqual(md5_out, md5_expected)

    def test_get_cached(self):
        chunk = {"Path": "abc-123/def_456", "ContentMd5": "md5-123"}

        df = pd.DataFrame({"values": [1.0, 2.0, 3.0]}, index=[1, 2, 3])

        cache = StorageCache()
        with patch.multiple(
            cache, _get_cache_id_md5=DEFAULT, _get_cached_data=DEFAULT
        ) as mocks:
            mocks["_get_cache_id_md5"].return_value = (
                "abc123def456",
                _encode_for_path_safety("md5-123"),
            )
            mocks["_get_cached_data"].return_value = df

            data = cache.get(chunk)

            pd.testing.assert_frame_equal(df, data)
            mocks["_get_cache_id_md5"].assert_called_once_with(chunk)
            mocks["_get_cached_data"].assert_called_once_with(
                "abc123def456", _encode_for_path_safety("md5-123")
            )

    @patch("datareservoirio.storage.storage._blob_to_df")
    def test_get_not_cached(self, mock_remote_get):
        chunk = {
            "Path": "abc-123/def_456",
            "ContentMd5": "md5-123",
            "Endpoint": "abc-123/def_456?start=12",
        }

        df = pd.DataFrame({"values": [1.0, 2.0, 3.0]}, index=[1, 2, 3])

        cache = StorageCache()
        with patch.multiple(
            cache, _get_cache_id_md5=DEFAULT, _get_cached_data=DEFAULT
        ) as mocks:
            mocks["_get_cache_id_md5"].return_value = (
                "abc123def456",
                _encode_for_path_safety("md5-123"),
            )
            mocks["_get_cached_data"].return_value = None
            mock_remote_get.return_value = df

            data = cache.get(chunk)

            assert data is None
            mocks["_get_cache_id_md5"].assert_called_once_with(chunk)
            mocks["_get_cached_data"].assert_called_once_with(
                "abc123def456", _encode_for_path_safety("md5-123")
            )

    def test_put_data_to_cache_tiny(self):
        chunk = {"Path": "abc123def456", "ContentMd5": "md5-123"}
        df = pd.DataFrame({"values": [1.0, 2.0, 3.0]}, index=[1, 2, 3])

        cache = StorageCache()
        cache.put(df, chunk)
        self._cacheindex_patch._get_filepath.assert_not_called()

    def test_put_data_to_cache(self):
        chunk = {"Path": "abc123def456", "ContentMd5": "md5-123"}

        df = pd.DataFrame(
            {"values": np.arange(24 * 61)}, index=np.arange(24 * 61, dtype=int)
        )

        cache = StorageCache()
        with patch.object(cache, "_write") as mock_write:
            cache.put(df, chunk)

        self.assertEqual(self._evict_patch.call_count, 2)
        mock_write.assert_called_once()

    @patch("os.path.exists")
    @patch("shutil.rmtree")
    def test_evict_entry_root(self, mock_rmtree, mock_exists):
        mock_exists.return_value = False
        cache = StorageCache()
        cache._evict_entry_root("root")

        mock_rmtree.assert_called_once_with("root")
        self._makedirs_patch.assert_called_with("root")

    def test_evict_entry(self):
        cache = StorageCache()
        with patch.object(cache, "_delete") as mock_delete:
            cache._evict_entry("id_", "md5")
        mock_delete.assert_called()

    def test_get_cached_data_not_in_cache(self):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")

        cache = StorageCache()
        with patch.object(cache._cache_index, "exists") as mock_exists:
            mock_exists.return_value = False

            self.assertIsNone(cache._get_cached_data(id_, md5))

    @patch("os.path.exists")
    def test_get_cached_data_full_match(self, mock_exists):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")
        df = pd.DataFrame(
            {"values": np.arange(24 * 61)}, index=np.arange(24 * 61, dtype=int)
        )

        cache = StorageCache()
        with patch.object(cache, "_cache_index") as mock_index:
            mock_index.exists.return_value = True
            mock_index.__getitem__.return_value = {
                "md5": _encode_for_path_safety("md5-123")
            }

            with patch.object(cache, "_read") as mock_read:
                mock_read.return_value = df

                df_out = cache._get_cached_data(id_, md5)
        pd.testing.assert_frame_equal(df, df_out)
        mock_index.touch.assert_called_once_with(id_, md5)

    @patch("os.path.exists")
    def test_get_cached_data_file_missing(self, mock_exists):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")
        df = pd.DataFrame(
            {"values": np.arange(24 * 61)}, index=np.arange(24 * 61, dtype=int)
        )

        cache = StorageCache()
        with patch.object(cache, "_cache_index") as mock_index:
            mock_index.exists.return_value = False
            mock_index.__getitem__.return_value = {
                "md5": _encode_for_path_safety("md5-123")
            }

            with patch.multiple(cache, _read=DEFAULT, _evict_entry=DEFAULT) as mocks:
                mocks["_read"].return_value = df

                self.assertIsNone(cache._get_cached_data(id_, md5))


class Test__blob_to_series:
    def test_get_numeric(
        self, mock_requests_get, numeric_blob_file_path, numeric_blob_df
    ):
        df_out = _blob_to_df(numeric_blob_file_path)
        pd.testing.assert_frame_equal(df_out, numeric_blob_df)

    def test_get_str(self, mock_requests_get, str_blob_file_path, str_blob_df):
        df_out = _blob_to_df(str_blob_file_path)
        pd.testing.assert_frame_equal(df_out, str_blob_df)


class Test__df_to_blob:
    @patch(
        "requests.put", **{"return_value.raise_for_status.return_value": MagicMock()}
    )
    def test_put_numeric(
        self, mock_put, numeric_blob_file_path, numeric_blob_df, bytesio_with_memory
    ):
        with patch("io.BytesIO", new=bytesio_with_memory):
            _df_to_blob(numeric_blob_df, "http:://azure.com/myblob")

            mock_put.return_value.raise_for_status.assert_called_once()

            with open(numeric_blob_file_path, "rb") as fp:
                file_expected = fp.read()

            call_args = mock_put.call_args
            assert call_args.args == ("http:://azure.com/myblob",)
            assert call_args.kwargs["headers"] == {"x-ms-blob-type": "BlockBlob"}
            assert call_args.kwargs["data"].memory == file_expected.replace(
                b"\r\n", b"\n"
            )

    @patch(
        "requests.put", **{"return_value.raise_for_status.return_value": MagicMock()}
    )
    def test_put_str(
        self, mock_put, str_blob_file_path, str_blob_df, bytesio_with_memory
    ):
        with patch("io.BytesIO", new=bytesio_with_memory):
            _df_to_blob(str_blob_df, "http:://azure.com/myblob")

            mock_put.return_value.raise_for_status.assert_called_once()

            with open(str_blob_file_path, "rb") as fp:
                file_expected = fp.read()

            call_args = mock_put.call_args
            assert call_args.args == ("http:://azure.com/myblob",)
            assert call_args.kwargs["headers"] == {"x-ms-blob-type": "BlockBlob"}
            assert call_args.kwargs["data"].memory == file_expected.replace(
                b"\r\n", b"\n"
            )

    @patch(
        "requests.put",
        **{"return_value.raise_for_status.side_effect": Exception("this_test")},
    )
    def test_put_raise(self, mock_put, numeric_blob_df):
        with pytest.raises(Exception) as e:
            _df_to_blob(numeric_blob_df, "http:://azure.com/myblob")
        assert "this_test" == str(e.value)


if __name__ == "__main__":
    unittest.main()
