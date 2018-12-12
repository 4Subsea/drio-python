from __future__ import absolute_import, division, print_function

import io
import logging
import os
import shutil
import threading
import warnings

from ..appdirs import user_cache_dir, WINDOWS, _win_path
from ..log import LogWriter

logger = logging.getLogger(__name__)
log = LogWriter(logger)


def _file_size_in_megabytes(filepath):
    """Return the file size in megabytes, or 0 if the file does not exist."""
    if not os.path.exists(filepath):
        return 0
    return os.path.getsize(filepath) / (1024.0 * 1024.0)


def _file_lastmodified_time(filepath):
    """"Return the file's last modified time or 0 if the file does not exist."""
    if not os.path.exists(filepath):
        return 0
    return os.path.getmtime(filepath)


class SimpleFileCache(object):
    """
    Cache implementation that stores files in the current user account's
    local profile.
    """

    STOREFORMATVERSION = 'v1'

    def __init__(self, max_size=1024, cache_root=None,
                 cache_folder='datareservoirio'):
        """
        Cache implementation that stores files in the current account's
        profile.

        By default, the store will be placed in a folder in the LOCALAPPDATA
        environment variable. If this variable is not available, the store will
        be placed in the temporary file location. Will scavenge the cache based
        on total file size.

        Parameters
        ---------
        max_size : int
            When cache reaches this limit, old files will be removed.
        cache_root : string
            The root location where cache is stored. Defaults to the
            LOCALAPPDATA environment variable.
        cache_folder : string
            Base folder within the default cache_root where cached data is
            stored. If cache_root is specified, this parameter is ignored.

        """
        self._max_size_MB = max_size
        self._evicter = EvictBySizeAndAge()
        self._evict_lock = threading.Lock()
        self._current_size = None
        self._init_cache(cache_root, cache_folder)

    def _init_cache(self, cache_root, cache_folder):
        if cache_root is None:
            cache_folder = cache_folder if cache_folder else ''
            root = user_cache_dir(cache_folder)
        else:
            root = cache_root
        self._root = os.path.abspath(root)

        if not os.path.exists(self.cache_root):
            os.makedirs(self.cache_root)

        self._evict_from_cache()
        EvictBySizeAndAge.CLEANUP = False

    @property
    def _cache_hive(self):
        return self.STOREFORMATVERSION

    @property
    def cache_root(self):
        """Root folder where data is cached."""
        return self._root

    @property
    def cache_size(self):
        """Get current (estimated) cache size."""
        return self._current_size

    def reset_cache(self):
        """Reset the cache, deleting any stored data."""
        self._evict_entry_root(self.cache_root)

    def get(self, get_data, serialize_data, deserialize_data, *tokens):
        """
        Retrieve data from cache based on tokens identifying the cache entry.

        Parameters
        ---------
        get_data : callable
            Method that will retreive the un-cached data. Will be called when
            the cache does not contain the value,
            or the value needs to be refreshed.
        serialize_data : callable
            Method that accept data and stream that is to be stored to disk.
        deserialize_data : callable
            Method that accept a stream and must deserialize and
            return the corresponding data.
        tokens : args
            List of one or more elements that together will form the key
            for this cache element.

        """
        if not tokens:
            raise ValueError('Expects one or more tokens that identify the data element')

        filepath = self._get_cache_filepath_for(*tokens)
        log.debug('Cache lookup {}'.format(filepath))

        data = self._get_cached_data(filepath, deserialize_data)
        if data is None:
            log.debug('Cache miss on {}'.format(filepath))
            data = get_data()
            if len(data) > 60*24:
                self._put_data_to_cache(data, filepath, serialize_data)
        else:
            log.debug('Cache hit on {}'.format(filepath))

        return data

    def _get_cache_filepath_for(self, *tokens):
        fileroot = os.path.normpath(
            os.path.join(self.cache_root, self._cache_hive, *tokens))
        return fileroot

    def _put_data_to_cache(self, data, filepath, serialize_data):
        root = os.path.dirname(filepath)
        if not os.path.exists(root):
            os.makedirs(root)
        else:
            self._evict_entry_root(root)

        self._evict_from_cache()

        new_megabytes = self._write_to_cache(data, filepath, serialize_data)
        self._current_size += new_megabytes

    def _get_cached_data(self, filepath, deserialize_data):
        if os.path.exists(filepath):
            log.debug('Trying to load from {}'.format(filepath))
            return self._read_from_cache(filepath, deserialize_data)

    def _evict_entry_root(self, root):
        log.debug('Resetting {}'.format(root))
        shutil.rmtree(root)
        if not os.path.exists(root):
            os.makedirs(root)

    def _evict_from_cache(self):
        log.debug('Current cache disk usage (estimate): {} of {}'.format(
            self.cache_size, self._max_size_MB))

        # Thread-safe cache eviction using a double-check pattern
        if self.cache_size is not None and self.cache_size < self._max_size_MB:
            return self.cache_size

        with self._evict_lock:
            if self.cache_size is not None and self.cache_size < self._max_size_MB:
                return self.cache_size

            log.debug('Analyzing storage for eviction. Max size {} in {}'.format(
                self._max_size_MB, self.cache_root))

            self._current_size = self._evicter.evict(
                self.cache_root, self._max_size_MB)

            log.debug('Storage analyzed. Current size: {} in {}'.format(
                self._current_size, self.cache_root))

    def _write_to_cache(self, data, filepath, serialize_data):
        pre_filepath = filepath + '.uncommitted'
        if WINDOWS:
            pre_filepath = _win_path(pre_filepath)

        with io.open(pre_filepath, 'wb') as file_:
            try:
                serialize_data(data, file_)
            except Exception as error:
                log.error('Serialize to {} failed with exception: {}'.format(
                    pre_filepath, error))
                raise

        os.rename(pre_filepath, filepath)
        return _file_size_in_megabytes(filepath)

    def _read_from_cache(self, filepath, deserialize_data):
        if WINDOWS:
            filepath = _win_path(filepath)

        with io.open(filepath, 'rb') as file_:
            return deserialize_data(file_)


class EvictBySizeAndAge(object):
    """
    Cache eviction based on total size of a folder.

    When a max size is reached, the oldest files will be deleted until total
    size is below the maximum. Files will be considered the same age when
    created within the same hour, so that the largest file created within one
    hour will be evicted before small files.
    """
    CLEANUP = True

    def evict(self, folder, max_size_mb):
        """
        Evict potential files from `folder` based on total file size and
        maximum file size.
        """
        entries = []
        for root, dirs, filenames in os.walk(folder):
            if self.CLEANUP and not dirs and not filenames:
                log.info('Trying to evict empty root from storage: {}'.format(root))
                try:
                    os.rmdir(root)
                except Exception as error:
                    warnings.warn('Could not evict {}. Exception: {}'.format(root, error))
                continue

            for filename in filenames:
                filepath = os.path.join(root, filename)
                entries.append({
                    'filepath': filepath,
                    'size': _file_size_in_megabytes(filepath),
                    'time': _file_lastmodified_time(filepath) // (60 * 60)
                    })

        totalsize_mb = sum(e['size'] for e in entries)
        overage_mb = totalsize_mb - max_size_mb

        if overage_mb > 0:
            log.info('Storage is {}MB over max size {}MB. evicting old files'.format(
                overage_mb, max_size_mb))
            sorted_by_age = sorted(entries,
                                   key=lambda e: (e['time'], -1.*e['size']))
            files_to_evict = []
            size_mb = 0
            for e in sorted_by_age:
                size_mb += e['size']
                files_to_evict.append(e['filepath'])
                log.debug('Schedule for eviction {}'.format(e))
                if size_mb >= overage_mb:
                    break

            for e in files_to_evict:
                if os.path.exists(e):
                    log.info('Trying to evict {} from storage'.format(e))
                    try:
                        os.remove(e)
                        os.rmdir(os.path.dirname(e))
                    except Exception as error:
                        warnings.warn('Could not evict {}. Exception: {}'.format(e, error))

            return totalsize_mb - size_mb
        return totalsize_mb
