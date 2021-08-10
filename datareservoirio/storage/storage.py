import base64
import logging
import os
import re
import shutil
import timeit
from concurrent.futures import ThreadPoolExecutor
from threading import RLock as Lock

import pandas as pd

from ..appdirs import user_cache_dir
from .cache_engine import CacheIO, _CacheIndex
from .storage_engine import StorageBackend

log = logging.getLogger(__name__)


def _encode_for_path_safety(value):
    return str(base64.urlsafe_b64encode(str(value).encode()).decode())


class Storage:
    """
    Handle download and upload of timeseries data in DataReservoir.io.
    """

    def __init__(self, timeseries_api, files_api, downloader, uploader):
        """
        Initialize service for working with timeseries data in Azure Blob
        Storage.

        Parameters
        ----------
        timeseries_api: TimeseriesAPI
            Instance of timeseries API.
        files_api: FilesAPI
            Instance of files API.
        downloader: cls
            A strategy instance for handling downloads.
        uploader: cls
            A strategy instance for handling uploads.

        """
        self._timeseries_api = timeseries_api
        self._files_api = files_api
        self._downloader = downloader
        self._uploader = uploader

    def put(self, series):
        """
        Put a data into storage.

        Parameters
        ----------
        series : pandas.Series
            pandas Series to store

        Returns
        -------
            The unique file id as stored in the reservoir

        """
        upload_params = self._files_api.upload()
        file_id = upload_params["FileId"]

        self._uploader.put(upload_params, series)

        self._files_api.commit(file_id)
        return file_id

    def get(self, timeseries_id, start, end):
        """
        Get a range of data for a timeseries.

        Parameters
        ----------
        timeseries_id: guid
            Unique id of the timeseries
        start: long
            Start time of the range, in nanoseconds since EPOCH
        end: long
            End time of the range, in nanoseconds since EPOCH

        """
        log.debug("getting day file inventory")
        response = self._timeseries_api.download_days(timeseries_id, start, end)

        df = self._downloader.get(response)
        print(df.dtypes)

        # at this point, an entirely new object w/o reference
        # to internal objects is created.
        if df.empty:
            return pd.Series()
        return self._create_series(df, start, end)

    def _create_series(self, df, start, end):
        """Create a new pandas Series w/o internal references"""
        index_bool = (df.index.values >= start) & (df.index.values <= end)
        index = df.index.values[index_bool]  # enforces copy
        values = df.values[index_bool, 0]  # enforces copy
        dtype = df.dtypes[0]
        return pd.Series(data=values, index=index, dtype=dtype)


class BaseDownloader:
    """
    Series download strategy that will download Pandas DataFrame from provided
    cloud backend.

    Multiple chunks will be downloaded in parallel using ThreadPoolExecutor.

    """

    def __init__(self, backend):
        self._backend = backend

    def get(self, response):
        filechunks = [f["Chunks"] for f in response["Files"]]
        filedatas = map(self._download_chunks_as_dataframe, filechunks)

        try:
            df = next(filedatas)
        except StopIteration:
            return pd.DataFrame()

        for fd in filedatas:
            df = self._combine_first(fd, df)
        return df

    def _download_chunks_as_dataframe(self, chunks):
        if not chunks:
            return pd.DataFrame()

        with ThreadPoolExecutor() as executor:
            filechunks = executor.map(self._download_verified_chunk, chunks)
        df_chunks = pd.concat(filechunks)
        return df_chunks

    def _download_verified_chunk(self, chunk):
        """
        Download chunk as pandas DataFrame and ensure the series does not contain
        duplicates.
        """
        df = self._backend.get(chunk)
        if not df.index.is_unique:
            return df[~df.index.duplicated(keep="last")]
        return df

    @staticmethod
    def _combine_first(calling, other):
        """
        Faster combine first for most common scenarios and fall back to general
        purpose pandas combine_first for advanced cases.
        """
        if calling.empty or other.empty:
            return calling

        calling_index = calling.index.values
        other_index = other.index.values

        late_start = max([calling_index[0], other_index[0]])
        early_end = min([calling_index[-1], other_index[-1]])

        if late_start > early_end:  # no overlap
            if calling_index[0] < late_start:
                df_combined = pd.concat((calling, other))
            else:
                df_combined = pd.concat((other, calling))
        elif (
            len(calling_index) == len(other_index)
            and (calling_index == other_index).all()
        ):  # exact overlap
            df_combined = calling
        else:  # partial overlap - expensive
            df_combined = calling.combine_first(other)

        return df_combined


class BaseUploader:
    """
    Series upload strategy that will upload Pandas Series to provided cloud
    backend.

    """

    def __init__(self, backend):
        self._backend = backend

    def put(self, params, data):
        return self._backend.put(params, data)


class FileCacheDownload(CacheIO, StorageBackend):
    """
    Backend for download with file based cache.

    By default, the store will be placed in a folder in the LOCALAPPDATA
    environment variable. If this variable is not available, the store will
    be placed in the temporary file location. Will scavenge the cache based
    on total file size.

    In addition, the most recent calls will be cached in memory to reduce
    disk I/O during repetetive requests towards the same data.

    Parameters
    ---------
    max_size : int
        When cache reaches this limit (in MB), old files will be removed.
    cache_root : string
        The root location where cache is stored. Defaults to the
        LOCALAPPDATA environment variable.
    cache_folder : string
        Base folder within the default cache_root where cached data is
        stored. If cache_root is specified, this parameter is ignored.
    format_ : string
        Specify cache file format. 'parquet' or 'csv'.
        Default is 'parquet'.

    """

    STOREFORMATVERSION = "v2"
    CACHE_THRESHOLD = 24 * 60  # number of rows

    def __init__(
        self,
        max_size=1024,
        cache_root=None,
        cache_folder="datareservoirio",
        format_="parquet",
    ):
        self._max_size = max_size * 1024 * 1024
        self._cache_format = format_

        self._init_cache_dir(cache_root, cache_folder)
        self._cache_index = _CacheIndex(self._cache_path, self._max_size)

        self._evict_lock = Lock()
        self._evict_from_cache()

        super().__init__(format_)

    def _init_cache_dir(self, cache_root, cache_folder):
        if cache_root is None:
            cache_folder = cache_folder if cache_folder else ""
            root = user_cache_dir(cache_folder)
        else:
            root = cache_root
        self._root = os.path.abspath(root)

        if not os.path.exists(self._cache_path):
            os.makedirs(self._cache_path)

    @property
    def _cache_hive(self):
        return self.STOREFORMATVERSION

    @property
    def cache_root(self):
        """Root folder where data is cached."""
        return self._root

    @property
    def _cache_path(self):
        return os.path.join(self.cache_root, self._cache_hive)

    def reset_cache(self):
        """Reset the cache, deleting any stored data."""
        self._evict_entry_root(self.cache_root)

    def get(self, chunk):
        """
        Retrieve data from backend. Uses cached data if it is available.

        Parameters
        ---------
        chunk : dict
            Dictionary containing parameters required by the backend to get
            data.

        """
        id_, md5 = self._get_cache_id_md5(chunk)
        log.debug(f"Cache lookup {id_}")

        data = self._get_cached_data(id_, md5)
        if data is None:
            log.debug(f"Cache miss on {id_}")
            data = super().remote_get(chunk)
            self._put_data_to_cache(data, id_, md5)
        else:
            log.debug(f"Cache hit on {id_}")
        return data

    def _get_cache_id_md5(self, chunk):
        path = chunk["Path"]
        md5 = _encode_for_path_safety(chunk["ContentMd5"])
        id_ = re.sub(r"-|_|/|\.", "", path)
        return self._cache_format + id_, md5  # modify id_ with format prefix

    def _put_data_to_cache(self, data, id_, md5):
        if len(data) <= self.CACHE_THRESHOLD:
            return  # do not cache tiny files
        filepath = self._cache_index._get_filepath(id_, md5)
        self._write(data, filepath)
        self._cache_index._register_file(id_, md5)
        self._evict_from_cache()

    def _get_cached_data(self, id_, md5):
        if not self._cache_index.exists(id_, md5):
            return

        filepath = self._cache_index._get_filepath(id_, md5)

        log.debug(f"Loading cached data from {filepath}")

        data = self._read(filepath)
        self._cache_index.touch(id_, md5)

        return data

    def _evict_entry_root(self, root):
        log.debug(f"Resetting {root}")
        shutil.rmtree(root)
        if not os.path.exists(root):
            os.makedirs(root)

    def _evict_entry(self, id_, md5):
        filepath = self._cache_index._get_filepath(id_, md5)
        self._delete(filepath)

    def _evict_from_cache(self):
        log.debug(
            f"Current cache disk usage (estimate): {self._cache_index.size} of {self._max_size}"
        )

        # Thread-safe cache eviction using a double-check pattern
        if self._cache_index.size_less_than_max:
            return

        with self._evict_lock:
            if self._cache_index.size_less_than_max:
                return

            log.debug(
                f"Analyzing storage for eviction. Max size {self._cache_index._max_size} in {self.cache_root}"
            )

            time_start = timeit.default_timer()

            while not self._cache_index.size_less_than_max:
                id_, item = self._cache_index.popitem()
                self._evict_entry(id_, item["md5"])

            time_end = timeit.default_timer()
            log.debug(
                f"Storage analyzed (in {time_end - time_start:.2f} seconds). Current size: {self._cache_index.size} in {self.cache_root}"
            )


class DirectDownload(StorageBackend):
    """
    Backend for direct download from cloud.

    """

    def get(self, chunk):
        """
        Retrieve data.

        Parameters
        ---------
        chunk : dict
            Dictionary containing parameters required by the backend to get
            data.

        """
        return super().remote_get(chunk)


class DirectUpload(StorageBackend):
    """
    Backend for direct upload to cloud.

    """

    def put(self, params, data):
        return super().remote_put(params, data)
