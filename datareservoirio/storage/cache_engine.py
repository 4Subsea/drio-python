import codecs
import io
import logging
import os
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict

import pandas as pd

from ..appdirs import WINDOWS, _win_path

log = logging.getLogger(__name__)


_BYTES_PER_ROW = 128 // 8


class GenericFormat(metaclass=ABCMeta):
    """
    Abstract class for file format classes.
    """

    @abstractproperty
    def file_extension(self):
        pass

    @abstractmethod
    def serialize(self, dataframe, stream):
        pass

    @abstractmethod
    def deserialize(self, stream):
        pass


class CsvFormat(GenericFormat):
    """Serialize dataframe to/from the csv format."""

    def __init__(self):
        self._reader_factory = codecs.getreader("utf-8")
        self._writer_factory = codecs.getwriter("utf-8")

    @property
    def file_extension(self):
        return "csv"

    def serialize(self, dataframe, stream):
        with self._writer_factory(stream) as sw:
            dataframe.to_csv(sw, header=True, encoding="ascii")

    def deserialize(self, stream):
        with self._reader_factory(stream) as sr:
            return pd.read_csv(sr, index_col=0, encoding="ascii")


class ParquetFormat(GenericFormat):
    """Serialize dataframe to/from the parquet format."""

    @property
    def file_extension(self):
        return "parquet"

    def serialize(self, dataframe, stream):
        dataframe.to_parquet(stream)

    def deserialize(self, stream):
        return pd.read_parquet(stream)


class CacheIO:
    """
    Basic cache related disk operations.

    Parameter
    ---------
    format : str
        Which format to use when storing files in the cache. Accepts `parquet`
        (recommended) for Parquet, and `csv` for CSV.

    """

    def __init__(self, format_, *args, **kwargs):
        if format_ == "parquet":
            self._io_backend = ParquetFormat()
        elif format_ == "csv":
            self._io_backend = CsvFormat()
        else:
            raise ValueError("Unreckognized format: {}".format(format))
        super().__init__(*args, **kwargs)

    def _write(self, data, filepath):
        pre_filepath = filepath + ".uncommitted"
        with io.open(pre_filepath, "wb") as file_:
            try:
                log.debug(f"Write {pre_filepath}")
                self._io_backend.serialize(data, file_)
            except Exception as error:
                log.exception(f"Serialize to {pre_filepath} failed: {error}")
                raise
        log.debug(f"Commit {pre_filepath} as {filepath}")
        os.rename(pre_filepath, filepath)

    def _read(self, filepath):
        with io.open(filepath, "rb") as file_:
            data = self._io_backend.deserialize(file_)
        os.utime(filepath)
        return data

    def _delete(self, filepath):
        try:
            log.debug(f"Evict {filepath}")
            os.remove(filepath)
        except Exception as error:
            log.exception(f"Could not delete {filepath}: {error}")


class _CacheIndex(OrderedDict):
    """
    Keep track of cache index in-memory.
    """

    def __init__(self, cache_path, max_size):
        self._cache_path = cache_path
        self._max_size = max_size

        cache_index_list = []
        for file_ in os.scandir(self._cache_path):
            stat = file_.stat()
            id_, md5 = file_.name.split("_")
            cache_index_list.append(
                (
                    self._key(id_, md5),
                    self._index_item(id_, md5, stat.st_size, stat.st_mtime),
                )
            )
        cache_index_list.sort(key=lambda item: item[1]["time"])
        super(_CacheIndex, self).__init__(cache_index_list)
        self._update_size()

    def exists(self, id_, md5):
        """Check if the entry exist in the cache."""
        key = self._key(id_, md5)
        entry_exist = key in self
        file_exist = self._file_exists(id_, md5)

        if not entry_exist and not file_exist:
            return False
        elif not entry_exist and file_exist:
            self._register_file(id_, md5)
            return True
        elif entry_exist and not file_exist:
            del self[key]
            return False

        return True

    def touch(self, id_, md5):
        """Mark the entry as recently used."""
        key = self._key(id_, md5)
        if key in self:
            self.move_to_end(key)

    @property
    def size_less_than_max(self):
        """
        Check if the current cache size is less than the maximum allowed size.
        """
        return self.size < self._max_size

    @property
    def size(self):
        """Current cache size."""
        return self._current_size

    def _update_size(self):
        self._current_size = 0
        for item in self.values():
            self._current_size += item["size"]

    def popitem(self):
        key, item = super(_CacheIndex, self).popitem(last=False)
        self._current_size -= item["size"]
        return item["id"], item

    @staticmethod
    def _key(id_, md5):
        return f"{id_}_{md5}"

    @staticmethod
    def _index_item(id_, md5, size, time):
        item = {"id": id_, "md5": md5, "size": size, "time": time}
        return item

    def _get_filepath(self, id_, md5):
        filepath = os.path.normpath(
            os.path.join(self._cache_path, "_".join([id_, md5]))
        )
        if WINDOWS:
            filepath = _win_path(filepath)
        return filepath

    def _file_exists(self, id_, md5):
        return os.path.exists(self._get_filepath(id_, md5))

    def _register_file(self, id_, md5):
        filepath = self._get_filepath(id_, md5)
        stat = os.stat(filepath)
        item = self._index_item(id_, md5, stat.st_size, stat.st_mtime)
        self[self._key(id_, md5)] = item
        self._current_size += item["size"]
