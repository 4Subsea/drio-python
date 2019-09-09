import codecs
import io
import os
import unittest
import warnings
from unittest.mock import MagicMock, Mock, mock_open, patch

import pandas as pd

from datareservoirio.storage.cache_engine import (
    CacheIO, CsvFormat, GenericFormat, MsgPackFormat, ParquetFormat,
    _CacheIndex)


class Test_CsvFormat(unittest.TestCase):
    def test__init(self):
        file_format = CsvFormat()
        self.assertIsInstance(file_format, GenericFormat)

    def test_extension(self):
        file_format = CsvFormat()
        self.assertEqual(file_format.file_extension, "csv")

    @patch("datareservoirio.storage.cache_engine.pd")
    def test_deserialize(self, mock_pd):
        file_format = CsvFormat()
        file_format.deserialize(io.BytesIO())
        mock_pd.read_csv.assert_called_once()

    def test_deserialize_actual(self):
        df = pd.DataFrame({"values": [0, 1, 2, 3, 4]}, index=[0, 1, 2, 3, 4])
        stream = codecs.getwriter("utf-8")(io.BytesIO())
        df.to_csv(stream)
        stream.seek(0)

        file_format = CsvFormat()
        df_out = file_format.deserialize(stream)
        pd.testing.assert_frame_equal(df, df_out)

    def test_serialize(self):
        file_format = CsvFormat()
        mock_df = MagicMock()
        stream = io.BytesIO()

        file_format.serialize(mock_df, stream)
        mock_df.to_csv.assert_called_once()


class Test_MsgPackFormat(unittest.TestCase):
    """DEPRECATED"""

    def test__init(self):
        file_format = MsgPackFormat()
        self.assertIsInstance(file_format, GenericFormat)

    def test_extension(self):
        file_format = MsgPackFormat()
        self.assertEqual(file_format.file_extension, "mp")

    @patch("datareservoirio.storage.cache_engine.pd")
    def test_deserialize(self, mock_pd):
        file_format = MsgPackFormat()

        mock_stream = io.BytesIO()
        file_format.deserialize(mock_stream)
        mock_pd.read_msgpack.assert_called_once_with(mock_stream)

    def test_deserialize_actual(self):
        df = pd.DataFrame({"values": [0, 1, 2, 3, 4]}, index=[0, 1, 2, 3, 4])
        stream = io.BytesIO()
        df.to_msgpack(stream)
        stream.seek(0)

        file_format = MsgPackFormat()
        df_out = file_format.deserialize(stream)
        pd.testing.assert_frame_equal(df, df_out)

    def test_serialize(self):
        file_format = MsgPackFormat()
        mock_df = MagicMock()
        stream = io.BytesIO()

        file_format.serialize(mock_df, stream)
        mock_df.to_msgpack.assert_called_once_with(stream)


class Test_ParquetFormat(unittest.TestCase):
    """DEPRECATED"""

    def test__init(self):
        file_format = ParquetFormat()
        self.assertIsInstance(file_format, GenericFormat)

    def test_extension(self):
        file_format = ParquetFormat()
        self.assertEqual(file_format.file_extension, "parquet")

    @patch("datareservoirio.storage.cache_engine.pd")
    def test_deserialize(self, mock_pd):
        file_format = ParquetFormat()

        mock_stream = io.BytesIO()
        file_format.deserialize(mock_stream)
        mock_pd.read_parquet.assert_called_once_with(mock_stream)

    def test_deserialize_actual(self):
        df = pd.DataFrame({"values": [0, 1, 2, 3, 4]}, index=[0, 1, 2, 3, 4])
        stream = io.BytesIO()
        df.to_parquet(stream)
        stream.seek(0)

        file_format = ParquetFormat()
        df_out = file_format.deserialize(stream)
        pd.testing.assert_frame_equal(df, df_out)

    def test_serialize(self):
        file_format = ParquetFormat()
        mock_df = MagicMock()
        stream = io.BytesIO()

        file_format.serialize(mock_df, stream)
        mock_df.to_parquet.assert_called_once_with(stream)


class Test_CacheIO(unittest.TestCase):
    def test__init(self):
        cache_io = CacheIO("parquet")
        self.assertIsInstance(cache_io._io_backend, ParquetFormat)

        cache_io = CacheIO("csv")
        self.assertIsInstance(cache_io._io_backend, CsvFormat)

        cache_io = CacheIO("msgpack")
        self.assertIsInstance(cache_io._io_backend, MsgPackFormat)

        with self.assertRaises(ValueError):
            CacheIO("unicorn")

    @patch("os.utime")
    def test_read(self, mock_utime):
        cache_io = CacheIO("parquet")

        df = pd.DataFrame({"values": [0, 1, 2, 3, 4]}, index=[0, 1, 2, 3, 4])
        stream = io.BytesIO()
        df.to_parquet(stream)
        stream.seek(0)

        with patch("io.open", mock_open()) as mopen:
            mopen.return_value = stream
            df_out = cache_io._read("file_path")

        mock_utime.assert_called_once_with("file_path")
        pd.testing.assert_frame_equal(df, df_out)

    @patch("os.remove")
    def test_delete(self, mock_remove):
        cache_io = CacheIO("parquet")
        cache_io._delete("file_path")

        mock_remove.assert_called_once_with("file_path")

        mock_remove.side_effect = Exception

        with warnings.catch_warnings(record=True) as w:
            cache_io._delete("file_path")
        assert len(w) == 1
        assert issubclass(w[-1].category, Warning)

    @patch("os.rename")
    def test_write(self, mock_rename):
        cache_io = CacheIO("parquet")
        df = MagicMock()

        with patch("io.open", mock_open()):
            cache_io._write(df, "file_path")
        df.to_parquet.assert_called_once()

        mock_rename.assert_called_once_with("file_path.uncommitted", "file_path")


class Test__CacheIndex(unittest.TestCase):
    def setUp(self):
        patcher = patch("os.scandir")
        self.mock_scandir = patcher.start()

        mock_file_0 = MagicMock(spec=os.DirEntry)
        mock_file_0.name = "id00_md500"
        mock_stat_result = Mock(spec=os.stat_result)
        mock_file_0.stat.return_value = mock_stat_result
        mock_stat_result.st_size = 128
        mock_stat_result.st_mtime = 2

        mock_file_1 = MagicMock(spec=os.DirEntry)
        mock_file_1.name = "id01_md501"
        mock_stat_result = Mock(spec=os.stat_result)
        mock_file_1.stat.return_value = mock_stat_result
        mock_stat_result.st_size = 128
        mock_stat_result.st_mtime = 0

        mock_file_2 = MagicMock(spec=os.DirEntry)
        mock_file_2.name = "id02_md502"
        mock_stat_result = Mock(spec=os.stat_result)
        mock_file_2.stat.return_value = mock_stat_result
        mock_stat_result.st_size = 128
        mock_stat_result.st_mtime = 1

        self.mock_scandir.return_value = [mock_file_0, mock_file_1, mock_file_2]

        self.addCleanup(patcher.stop)

    def test_init(self):
        cahce_index = _CacheIndex("./test", 128)

        for key in ["id00", "id01", "id02"]:
            self.assertIn(key, cahce_index)

        item_expected = {"md5": "md501", "size": 128, "time": 0}

        self.assertDictEqual(cahce_index["id01"], item_expected)
        self.assertListEqual(list(cahce_index.keys()), ["id01", "id02", "id00"])

    @patch("os.path.exists", return_value=False)
    def test_exists_false(self, mock_exists):
        cache_index = _CacheIndex("./test", 128)
        self.assertFalse(cache_index.exists("id04", "md502"))

    @patch("os.path.exists", return_value=True)
    def test_exists_true(self, mock_exists):
        cache_index = _CacheIndex("./test", 128)
        self.assertTrue(cache_index.exists("id02", "md502"))

    @patch("os.path.exists", return_value=False)
    def test_exists_true_no_file(self, mock_exists):
        cache_index = _CacheIndex("./test", 128)
        self.assertFalse(cache_index.exists("id02", "md502"))

        self.assertNotIn("id02", cache_index)

    @patch("os.path.exists", return_value=True)
    @patch("os.stat")
    def test_exists_false_w_file(self, mock_stat, mock_exists):
        mock_stat_result = Mock(spec=os.stat_result)
        mock_stat_result.st_size = 128
        mock_stat_result.st_mtime = 1

        mock_stat.return_value = mock_stat_result

        cache_index = _CacheIndex("./test", 128)
        self.assertTrue(cache_index.exists("id04", "md504"))

        self.assertIn("id04", cache_index)

    def test_size_less_than_max(self):
        cache_index = _CacheIndex("./test", 128 * 4)
        self.assertTrue(cache_index.size_less_than_max)

        cache_index = _CacheIndex("./test", 128 * 2)
        self.assertFalse(cache_index.size_less_than_max)

    def test_size(self):
        cache_index = _CacheIndex("./test", 128 * 4)
        self.assertEqual(cache_index.size, 3 * 128)

    def test__update_size(self):
        cache_index = _CacheIndex("./test", 128 * 4)
        cache_index._update_size()
        self.assertEqual(cache_index.size, 3 * 128)

        cache_index["id03"] = {"md5": "md503", "size": 128, "time": 0}

        cache_index._update_size()
        self.assertEqual(cache_index.size, 4 * 128)

        del cache_index["id00"]
        cache_index._update_size()
        self.assertEqual(cache_index.size, 3 * 128)

    def test_popitem(self):
        cache_index = _CacheIndex("./test", 128 * 4)
        id_out, item_out = cache_index.popitem()

        id_expected = "id01"
        item_expected = {"md5": "md501", "size": 128, "time": 0}

        self.assertEqual(id_out, id_expected)
        self.assertDictEqual(item_out, item_expected)

    def test__index_item(self):
        item_out = _CacheIndex._index_item("md504", 134, 34)
        item_expected = {"md5": "md504", "size": 134, "time": 34}

        self.assertDictEqual(item_out, item_expected)

    def test__get_filepath(self):
        cache_index = _CacheIndex("./test", 128 * 4)
        path_out = cache_index._get_filepath("id01", "md501")
        path_expected = os.path.normpath(os.path.join("./test", "id01_md501"))

        from datareservoirio.appdirs import _win_path, WINDOWS

        if WINDOWS:
            path_expected = _win_path(path_expected)

        self.assertEqual(path_out, path_expected)

    def test__file_exist(self):
        cache_index = _CacheIndex("./test", 128 * 4)

        with patch("os.path.exists") as mock_os_exists:
            mock_os_exists.return_value = True
            with patch.object(cache_index, "_get_filepath") as mock_get_filepath:
                mock_get_filepath.return_value = "./test/id01_md501"

                self.assertTrue(cache_index._file_exists("id01", "md501"))

        mock_get_filepath.assert_called_once_with("id01", "md501")
        mock_os_exists.assert_called_once_with("./test/id01_md501")

    @patch("os.stat")
    def test__register_file(self, mock_stat):
        mock_stat_result = Mock(spec=os.stat_result)
        mock_stat_result.st_size = 128
        mock_stat_result.st_mtime = 6

        mock_stat.return_value = mock_stat_result

        cache_index = _CacheIndex("./test", 128 * 4)
        cache_index._register_file("id05", "md505")

        self.assertIn("id05", cache_index)
        self.assertEqual(cache_index._current_size, 128 * 4)


if __name__ == "__main__":
    unittest.main()