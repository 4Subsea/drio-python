from __future__ import absolute_import

import os
import io
import logging
import shutil
import gzip

from ..log import LogWriter

logger = logging.getLogger(__name__)
log = LogWriter(logger)


class SimpleFileCache:
    """Cache implementation that stores files in the current account's local LOCALAPPDATA profile folder."""

    STOREFORMATVERSION = 'v1'
    DEFAULT_MAX_CACHE_SIZE_MB = 1024

    def __init__(self, max_size_MB=DEFAULT_MAX_CACHE_SIZE_MB, cache_root=None, cache_folder='reservoir_cache', compressionOn=True):
        """
        Cache implementation that stores files in the current account's LOCALAPPDATA profile folder.
        
        Will scavenge the cache based on total file size.

        Parameters
        ---------
        max_size_MB : int
            When cache reaches this limit, old files will be removed.
        cache_root : string
            The root location where cache is stored. Defaults to the LOCALAPPDATA environment variable.
        cache_folder : string
            Base folder within the default cache_root where cached data is stored.
            If cache_root is specified, this parameter is ignored.

        """
        self._max_size_MB = max_size_MB
        self._evicter = EvictBySizeAndAge()
        self._compressionOn = compressionOn

        root = cache_root if cache_root != None else os.environ['LOCALAPPDATA']
        folder = cache_folder if cache_root == None else None
        self._root = self._init_cache(root, folder)

    def _init_cache(self, root, folder):
        root = root if folder == None else os.path.join(root, folder)
        if not os.path.exists(root):
            os.makedirs(root)
        return root

    @property
    def _cache_hive(self):
        return self.STOREFORMATVERSION + ('gz' if self._compressionOn else 'raw')

    @property
    def cache_root(self):
        """Root folder where data is cached."""
        return self._root

    @property
    def enable_compression(self):
        """Compression enabled or not."""
        return self._compressionOn

    @enable_compression.setter
    def enable_compression(self, value):
        """Enable or disable compression."""
        self._compressionOn = value

    def reset_cache(self):
        """Reset the cache, deleting any stored data."""
        self._evict_entry_root(self._root)

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
        if not tokens or tokens is None or tokens.count == 0:
            raise ValueError('Expects one or more tokens that identify the data element')

        filepath = self._get_cache_filepath_for(*tokens)
        log.debug('Cache lookup {}'.format(filepath))

        data = self._get_cached_data(filepath, deserialize_data)
        if data is None:
            log.debug('Cache miss on {}'.format(filepath))
            data = get_data()
            self._put_data_to_cache(data, filepath, serialize_data)
        else:
            log.debug('Cache hit on {}'.format(filepath))

        return data

    def _get_cache_filepath_for(self, *tokens):
        fileroot = os.path.join(self._root, self._cache_hive, *tokens)
        return os.path.normpath(fileroot)

    def _put_data_to_cache(self, data, filepath, serialize_data):
        root = os.path.dirname(filepath)
        if not os.path.exists(root):
            os.makedirs(root)
        else:
            self._evict_entry_root(root)

        self._evict_from_cache()
        self._write_to_cache(data, filepath, serialize_data)

    def _get_cached_data(self, filepath, deserialize_data):
        if os.path.exists(filepath):
            log.debug('Trying to load from {}'.format(filepath))
            return self._read_from_cache(filepath, deserialize_data)

    def _evict_entry_root(self, root):
        log.debug('Clearing out {}'.format(root))
        shutil.rmtree(root)
        if not os.path.exists(root):
            log.debug('Recreating {}'.format(root))
            os.makedirs(root)

    def _evict_from_cache(self):
        log.debug('Analyzing storage for eviction. Max size {} in {}'.format(self._max_size_MB, self._root))
        self._evicter.evict(self._root, self._max_size_MB)

    def _write_to_cache(self, data, filepath, serialize_data):
        opener = gzip.open if self._compressionOn else io.open
        pre_filepath = filepath + '.uncommitted'
        with opener(pre_filepath, 'wb') as file:
            try:
                serialize_data(data, file)
            except Exception as error:
                log.error('Serialize to {} failed with exception: {}'.format(pre_filepath, error))
                raise
        os.rename(pre_filepath, filepath)

    def _read_from_cache(self, filepath, deserialize_data):
        opener = gzip.open if self._compressionOn else io.open
        with opener(filepath, 'rb') as file:
            return deserialize_data(file)

class EvictBySizeAndAge:
    """
    Cache eviction based on total size of a folder.
    
    When a max size is reached, the oldest files will
    be deleted until total size is below the maximum.
    """

    def evict(self, folder, max_size_MB):
        entries = [
            {'filepath': filepath, 'size': self._safe_getsize(filepath), 'time': self._safe_getmtime(filepath)}
            for filepath in [
                os.path.join(root, filename)
                for root, directories, filenames in os.walk(folder)
                for filename in filenames]]

        totalsizeMB = sum(e['size'] for e in entries) / (1024*1024)
        overageMB = totalsizeMB - max_size_MB

        if overageMB > 0:
            log.info('Storage is {}MB over max size {}MB. evicting old files...'.format(overageMB, max_size_MB))
            sorted_by_age = sorted(entries, key=lambda e: e['time'])
            files_to_evict = []
            sizeMB = 0
            for e in sorted_by_age:
                sizeMB += e['size']
                files_to_evict.append(e['filepath'])
                if sizeMB >= overageMB:
                    break

            for e in files_to_evict:
                if os.path.exists(e):
                    log.info('Trying to evict {} from storage'.format(e))
                    try:
                        os.remove(e)
                    except Exception as error:
                        log.warning('Could not evict {}, exception: '.format(e, error))

    @staticmethod
    def _safe_getsize(filepath):
        return 0 if not os.path.exists(filepath) else os.path.getsize(filepath)

    @staticmethod
    def _safe_getmtime(filepath):
        return 0 if not os.path.exists(filepath) else os.path.getmtime(filepath)    