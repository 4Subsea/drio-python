import os
import unittest
from unittest.mock import DEFAULT, MagicMock, Mock, PropertyMock, call, patch

import numpy as np
import pandas as pd
from requests import Session

from datareservoirio.appdirs import user_cache_dir
from datareservoirio.storage import (
    BaseDownloader,
    BaseUploader,
    DirectDownload,
    DirectUpload,
    FileCacheDownload,
    Storage,
)
from datareservoirio.storage.storage import _encode_for_path_safety


class Test_Storage(unittest.TestCase):
    def setUp(self):
        self._timeseries_api = Mock()
        self._files_api = Mock()
        self.downloader = Mock()
        self.uploader = Mock()

        self.tid = "abc-123-xyz"

        self.storage = Storage(
            self._timeseries_api,
            self._files_api,
            downloader=self.downloader,
            uploader=self.uploader,
        )

    def test_get(self):
        data_remote = pd.DataFrame([1, 2, 3, 4], index=[1, 2, 3, 4])
        self.downloader.get.return_value = data_remote

        series = self.storage.get(self.tid, 1, 10)

        self.assertTrue(series.equals(pd.Series([1, 2, 3, 4], index=[1, 2, 3, 4])))

    def test_put(self):
        self._files_api.upload.return_value = {"FileId": "42"}

        fileId = self.storage.put("data")

        self.assertEqual(fileId, "42")

    def test__create_series(self):
        df = pd.DataFrame(data=[0, 2, 4, 8, 16, 32, 64], index=[0, 2, 4, 6, 8, 10, 12])
        series_returned = self.storage._create_series(df, 4, 10)
        series_expected = pd.Series(data=[4, 8, 16, 32], index=[4, 6, 8, 10])
        pd.testing.assert_series_equal(series_returned, series_expected)
        self.assertFalse(series_returned._is_view)  # opps! private api...


class Test_DirectUpload(unittest.TestCase):
    @patch("datareservoirio.storage.storage.StorageBackend.__init__")
    def test_init(self, mock_backend):
        session = MagicMock(spec=Session)
        DirectUpload(session=session)
        mock_backend.assert_called_once()

    @patch("datareservoirio.storage.storage.StorageBackend.remote_put")
    def test_put(self, mock_remote_put):
        session = MagicMock(spec=Session)
        uploader = DirectUpload(session=session)
        uploader.put("params", "data")

        mock_remote_put.assert_called_once_with("params", "data")


class Test_DirectDownload(unittest.TestCase):
    @patch("datareservoirio.storage.storage.StorageBackend.__init__")
    def test_init(self, mock_backend):
        session = MagicMock(spec=Session)
        DirectDownload(session=session)
        mock_backend.assert_called_once()

    @patch("datareservoirio.storage.storage.StorageBackend.remote_get")
    def test_get(self, mock_remote_get):
        session = MagicMock(spec=Session)
        downloader = DirectDownload(session=session)
        downloader.get("params")

        mock_remote_get.assert_called_once_with("params")


class Test_BaseUploader(unittest.TestCase):
    def test_init(self):
        backend = MagicMock()
        uploader = BaseUploader(backend)

        self.assertEqual(backend, uploader._backend)

    def test_put(self):
        backend = MagicMock()
        uploader = BaseUploader(backend)
        uploader.put("params", "data")

        backend.put.assert_called_once_with("params", "data")


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
                pd.DataFrame([1.0, 2.0, 3.0], index=[1, 2, 3]),
                pd.DataFrame([5.0, 4.0, 5.0], index=[3, 4, 5]),
                pd.DataFrame([9.0, 8.0, 1.0], index=[1, 5, 9]),
            ]

            df_expected = pd.DataFrame(
                [9.0, 2.0, 5.0, 4.0, 8.0, 1.0], index=[1, 2, 3, 4, 5, 9]
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
                pd.DataFrame(["a", "b", "c"], index=[1, 2, 3]),
                pd.DataFrame(["d", "e", "f"], index=[3, 4, 5]),
                pd.DataFrame(["g", "h", "i"], index=[1, 5, 9]),
            ]

            df_expected = pd.DataFrame(
                ["g", "b", "d", "e", "h", "i"], index=[1, 2, 3, 4, 5, 9]
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
        mock_backend = MagicMock()
        mock_backend.get.return_value = pd.DataFrame([1.0, 2.0, 3.0], index=[1, 2, 3])

        df_expected = pd.DataFrame([1.0, 2.0, 3.0], index=[1, 2, 3])

        base_downloader = BaseDownloader(mock_backend)
        df_out = base_downloader._download_verified_chunk("chunk")
        mock_backend.get.assert_called_once_with("chunk")

        pd.testing.assert_frame_equal(df_expected, df_out)

    def test__download_verified_chunk_w_duplicates(self):
        mock_backend = MagicMock()
        mock_backend.get.return_value = pd.DataFrame(
            [1.0, 2.0, 4.0, 3.0], index=[1, 2, 2, 3]
        )

        df_expected = pd.DataFrame([1.0, 4.0, 3.0], index=[1, 2, 3])

        base_downloader = BaseDownloader(mock_backend)
        df_out = base_downloader._download_verified_chunk("chunk")
        mock_backend.get.assert_called_once_with("chunk")

        pd.testing.assert_frame_equal(df_expected, df_out)

    def test__download_chunks_as_dataframe(self):
        mock_backend = MagicMock()
        mock_backend.get.side_effect = [
            pd.DataFrame([1.0, 2.0, 3.0], index=[1, 2, 3]),
            pd.DataFrame([3.0, 4.0, 5.0], index=[4, 5, 6]),
            pd.DataFrame([5.0, 6.0, 7.0], index=[7, 8, 9]),
        ]

        df_expected = pd.DataFrame(
            [1.0, 2.0, 3.0, 3.0, 4.0, 5.0, 5.0, 6.0, 7.0],
            index=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        )

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

        storagebackend_patcher = patch(
            "datareservoirio.storage.storage.StorageBackend.__init__"
        )
        self._storagebackend_patch = storagebackend_patcher.start()

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

    @patch("datareservoirio.storage.storage.StorageBackend.remote_get")
    def test_get_not_cached(self, mock_remote_get):
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
            mocks["_get_cached_data"].return_value = None
            mock_remote_get.return_value = df

            data = cache.get(chunk)

            pd.testing.assert_frame_equal(df, data)
            mocks["_get_cache_id_md5"].assert_called_once_with(chunk)
            mocks["_get_cached_data"].assert_called_once_with(
                "abc123def456", _encode_for_path_safety("md5-123")
            )

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


if __name__ == "__main__":
    unittest.main()
