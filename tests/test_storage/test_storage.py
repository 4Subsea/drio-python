import io
import os
import unittest
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, Mock, call, patch

import numpy as np
import pandas as pd
import pytest

from datareservoirio.appdirs import user_cache_dir
from datareservoirio.storage import (

    BaseDownloader,
    DirectDownload,
    FileCacheDownload,
    Storage,
)
from datareservoirio.storage.storage import (
    _blob_to_df,
    _df_to_blob,
    _encode_for_path_safety,
)

TEST_PATH = Path(__file__).parent


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
        self._files_api = Mock()
        self.downloader = Mock()
        self.session = Mock()

        self.tid = "abc-123-xyz"

        self.storage = Storage(
            self._timeseries_api,
            downloader=self.downloader,
            session=self.session,
        )

    def test_get(self):
        data_remote = pd.DataFrame({"index": [1, 2, 3, 4], "values": [1, 2, 3, 4]})
        self.downloader.get.return_value = data_remote

        data_out = self.storage.get(self.tid, 2, 3)

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


class Test_DirectDownload(unittest.TestCase):
    @patch("datareservoirio.storage.storage._blob_to_df")
    def test_get(self, mock_remote_get):
        params = {"Endpoint": "https:://go-here-for-blob.com"}
        downloader = DirectDownload()
        downloader.get(params)

        mock_remote_get.assert_called_once_with(params["Endpoint"])


class Test_BaseDownloader(unittest.TestCase):
    def test_init(self):
        mock_backend = MagicMock()
        base_downloader = BaseDownloader(mock_backend)
        self.assertEqual(base_downloader._backend, mock_backend)

    def test_get(self):
        mock_backend = MagicMock()
        base_downloader = BaseDownloader(mock_backend)

        response = {
            "Files": [{"Chunks": "abc1"}, {"Chunks": "abc2"}, {"Chunks": "abc3"}]
        }

        with patch.object(
            base_downloader, "_download_chunks_as_dataframe"
        ) as mock_download:
            mock_download.side_effect = [
                pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]}).set_index(
                    "index"
                ),
                pd.DataFrame({"index": [3, 4, 5], "values": [5.0, 4.0, 5.0]}).set_index(
                    "index"
                ),
                pd.DataFrame({"index": [1, 5, 9], "values": [9.0, 8.0, 1.0]}).set_index(
                    "index"
                ),
            ]

            df_expected = pd.DataFrame(
                {"index": [1, 2, 3, 4, 5, 9], "values": [9.0, 2.0, 5.0, 4.0, 8.0, 1.0]}
            )
            df_out = base_downloader.get(response)

        pd.testing.assert_frame_equal(df_expected, df_out)

        calls = [call("abc1"), call("abc2"), call("abc3")]
        mock_download.assert_has_calls(calls)

    def test_get_with_text_data(self):
        mock_backend = MagicMock()
        base_downloader = BaseDownloader(mock_backend)

        response = {
            "Files": [{"Chunks": "abc1"}, {"Chunks": "abc2"}, {"Chunks": "abc3"}]
        }

        with patch.object(
            base_downloader, "_download_chunks_as_dataframe"
        ) as mock_download:
            mock_download.side_effect = [
                pd.DataFrame({"index": [1, 2, 3], "values": ["a", "b", "c"]}).set_index(
                    "index"
                ),
                pd.DataFrame({"index": [3, 4, 5], "values": ["d", "e", "f"]}).set_index(
                    "index"
                ),
                pd.DataFrame({"index": [1, 5, 9], "values": ["g", "h", "i"]}).set_index(
                    "index"
                ),
            ]

            df_expected = pd.DataFrame(
                {"index": [1, 2, 3, 4, 5, 9], "values": ["g", "b", "d", "e", "h", "i"]}
            )
            df_out = base_downloader.get(response)

        pd.testing.assert_frame_equal(df_expected, df_out)

    def test__combine_first_no_overlap(self):
        df1 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
        df2 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[6, 7, 8, 9])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloader._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__combine_first_no_overlap_reversed_order(self):
        df2 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
        df1 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[6, 7, 8, 9])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloader._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__combine_first_exact_overlap(self):
        df1 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
        df2 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[0, 1, 2, 3])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloader._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__combine_first_partial_overlap(self):
        df1 = pd.Series([0.0, 1.0, 2.0, 3.0], index=[0, 1, 2, 3])
        df2 = pd.Series([10.0, 11.0, 12.0, 13.0], index=[2, 3, 4, 5])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloader._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__download_verified_chunk(self):
        df = pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]})

        mock_backend = MagicMock()
        mock_backend.get.return_value = df

        df_expected = df.set_index("index")

        base_downloader = BaseDownloader(mock_backend)
        df_out = base_downloader._download_verified_chunk("chunk")
        mock_backend.get.assert_called_once_with("chunk")

        pd.testing.assert_frame_equal(df_expected, df_out)

    def test__download_verified_chunk_w_duplicates(self):
        df = pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]})
        mock_backend = MagicMock()
        mock_backend.get.return_value = df

        df_expected = df.set_index("index")

        base_downloader = BaseDownloader(mock_backend)
        df_out = base_downloader._download_verified_chunk("chunk")
        mock_backend.get.assert_called_once_with("chunk")

        pd.testing.assert_frame_equal(df_expected, df_out)

    def test__download_chunks_as_dataframe(self):
        mock_backend = MagicMock()
        mock_backend.get.side_effect = [
            pd.DataFrame({"index": [1, 2, 3], "values": [1.0, 2.0, 3.0]}),
            pd.DataFrame({"index": [4, 5, 6], "values": [3.0, 4.0, 5.0]}),
            pd.DataFrame({"index": [7, 8, 9], "values": [5.0, 6.0, 7.0]}),
        ]

        df_expected = pd.DataFrame(
            {
                "index": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "values": [1.0, 2.0, 3.0, 3.0, 4.0, 5.0, 5.0, 6.0, 7.0],
            }
        ).set_index("index")

        base_downloader = BaseDownloader(mock_backend)
        df_out = base_downloader._download_chunks_as_dataframe(
            ["chunk1", "chunk2", "chunk3"]
        )

        calls = [call("chunk1"), call("chunk2"), call("chunk3")]
        mock_backend.get.assert_has_calls(calls)

        pd.testing.assert_frame_equal(df_expected, df_out)


class Test_FileCachceDownload(unittest.TestCase):
    def setUp(self):
        makedirs_patcher = patch("datareservoirio.storage.storage.os.makedirs")
        self._makedirs_patch = makedirs_patcher.start()

        session_patcher = patch("requests.Session")
        self._session_patch = session_patcher.start()

        cacheio_patcher = patch("datareservoirio.storage.storage.CacheIO.__init__")
        self._cacheio_patch = cacheio_patcher.start()

        cacheindex_patcher = patch("datareservoirio.storage.storage._CacheIndex")
        self._cacheindex_patch = cacheindex_patcher.start()

        evict_patcher = patch(
            "datareservoirio.storage.FileCacheDownload._evict_from_cache"
        )
        self._evict_patch = evict_patcher.start()

        self.addCleanup(patch.stopall)

    def test_init(self):
        FileCacheDownload()
        self._evict_patch.assert_called_once()

    def test_cache_hive(self):
        cache = FileCacheDownload()
        self.assertEqual(cache._cache_hive, cache.STOREFORMATVERSION)

    def test_cache_root(self):
        cache = FileCacheDownload()
        self.assertEqual(
            cache.cache_root, os.path.abspath(user_cache_dir("datareservoirio"))
        )

        cache = FileCacheDownload(cache_folder="sometingelse")
        self.assertEqual(
            cache.cache_root, os.path.abspath(user_cache_dir("sometingelse"))
        )

        cache = FileCacheDownload(cache_root="home", cache_folder="anywhere")
        self.assertEqual(cache.cache_root, os.path.abspath("home"))

    def test_cache_path(self):
        cache = FileCacheDownload(cache_root="home")
        self.assertEqual(
            os.path.join(os.path.abspath("home"), cache.STOREFORMATVERSION),
            cache._cache_path,
        )

    def test_reset_cache(self):
        cache = FileCacheDownload()
        with patch.object(cache, "_evict_entry_root") as mock_evict:
            cache.reset_cache()
        mock_evict.assert_called_once_with(cache.cache_root)

    def test_get_cache_id_md5(self):
        chunk = {"Path": "abc-123/def_456", "ContentMd5": "md5-123"}

        cache = FileCacheDownload()
        id_out, md5_out = cache._get_cache_id_md5(chunk)
        id_expected = cache._cache_format + "abc123def456"
        md5_expected = _encode_for_path_safety("md5-123")
        self.assertEqual(id_out, id_expected)
        self.assertEqual(md5_out, md5_expected)

    def test_get_cached(self):
        chunk = {"Path": "abc-123/def_456", "ContentMd5": "md5-123"}

        df = pd.DataFrame({"values": [1.0, 2.0, 3.0]}, index=[1, 2, 3])

        cache = FileCacheDownload()
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

        cache = FileCacheDownload()
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

            pd.testing.assert_frame_equal(df, data)
            mocks["_get_cache_id_md5"].assert_called_once_with(chunk)
            mocks["_get_cached_data"].assert_called_once_with(
                "abc123def456", _encode_for_path_safety("md5-123")
            )
            mock_remote_get.assert_called_once_with(chunk["Endpoint"])

    def test_put_data_to_cache_tiny(self):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")
        df = pd.DataFrame({"values": [1.0, 2.0, 3.0]}, index=[1, 2, 3])

        cache = FileCacheDownload()
        cache._put_data_to_cache(df, id_, md5)
        self._cacheindex_patch._get_filepath.assert_not_called()

    def test_put_data_to_cache(self):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")
        df = pd.DataFrame(
            {"values": np.arange(24 * 61)}, index=np.arange(24 * 61, dtype=int)
        )

        cache = FileCacheDownload()
        with patch.object(cache, "_write") as mock_write:
            cache._put_data_to_cache(df, id_, md5)

        self.assertEqual(self._evict_patch.call_count, 2)
        mock_write.assert_called_once()

    @patch("os.path.exists")
    @patch("shutil.rmtree")
    def test_evict_entry_root(self, mock_rmtree, mock_exists):
        mock_exists.return_value = False
        cache = FileCacheDownload()
        cache._evict_entry_root("root")

        mock_rmtree.assert_called_once_with("root")
        self._makedirs_patch.assert_called_with("root")

    def test_evict_entry(self):
        cache = FileCacheDownload()
        with patch.object(cache, "_delete") as mock_delete:
            cache._evict_entry("id_", "md5")
        mock_delete.assert_called()

    def test_get_cached_data_not_in_cache(self):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")

        cache = FileCacheDownload()
        with patch.object(cache._cache_index, "exists") as mock_exists:
            mock_exists.return_value = False

            self.assertIsNone(cache._get_cached_data(id_, md5))

    @patch("os.path.exists")
    def test_get_cached_data_full_match(self, mock_exists):
        id_, md5 = "abc123def456", _encode_for_path_safety("md5-123")
        df = pd.DataFrame(
            {"values": np.arange(24 * 61)}, index=np.arange(24 * 61, dtype=int)
        )

        cache = FileCacheDownload()
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

        cache = FileCacheDownload()
        with patch.object(cache, "_cache_index") as mock_index:
            mock_index.exists.return_value = False
            mock_index.__getitem__.return_value = {
                "md5": _encode_for_path_safety("md5-123")
            }

            with patch.multiple(cache, _read=DEFAULT, _evict_entry=DEFAULT) as mocks:
                mocks["_read"].return_value = df

                self.assertIsNone(cache._get_cached_data(id_, md5))


class Test__blob_to_series:
    def test_get_numeric(self, numeric_blob_file_path, numeric_blob_df):
        df_out = _blob_to_df(numeric_blob_file_path)
        pd.testing.assert_frame_equal(df_out, numeric_blob_df)

    def test_get_str(self, str_blob_file_path, str_blob_df):
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
        **{"return_value.raise_for_status.side_effect": Exception("this_test")}
    )
    def test_put_raise(self, mock_put, numeric_blob_df):
        with pytest.raises(Exception) as e:
            _df_to_blob(numeric_blob_df, "http:://azure.com/myblob")
        assert "this_test" == str(e.value)


if __name__ == "__main__":
    unittest.main()
