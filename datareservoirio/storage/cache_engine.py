import io
import logging
import os
from collections import OrderedDict

import pandas as pd

from ..appdirs import WINDOWS, _win_path

log = logging.getLogger(__name__)


_BYTES_PER_ROW = 128 // 8


class CacheIO:
    """
    Basic cache related disk operations.

    """

    @staticmethod
    def _write(data, filepath):
        pre_filepath = filepath + ".uncommitted"
        with io.open(pre_filepath, "wb") as file_:
            try:
                log.debug(f"Write {pre_filepath}")
                data.to_parquet(file_)
            except Exception as error:
                log.exception(f"Serialize to {pre_filepath} failed: {error}")
                raise
        log.debug(f"Commit {pre_filepath} as {filepath}")
        os.rename(pre_filepath, filepath)

    @staticmethod
    def _read(filepath):
        with io.open(filepath, "rb") as file_:
            data = pd.read_parquet(file_)
        os.utime(filepath)
        return data

    @staticmethod
    def _delete(filepath):
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
