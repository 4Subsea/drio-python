import base64
import io
import logging
import os
import re
import shutil
import timeit
from concurrent.futures import ThreadPoolExecutor
from threading import RLock as Lock

import pandas as pd
import requests

from ..appdirs import user_cache_dir
from .cache_engine import CacheIO, _CacheIndex

log = logging.getLogger(__name__)


def _encode_for_path_safety(value):
    return str(base64.urlsafe_b64encode(str(value).encode()).decode())


class Storage:
    """
    Handle download and upload of timeseries data in DataReservoir.io.
    """

    def __init__(self, timeseries_api, session, cache=True, cache_opt=None):
        """
        Initialize service for working with timeseries data in Azure Blob
        Storage.

        Parameters
        ----------
        timeseries_api: TimeseriesAPI
            Instance of timeseries API.
        session : cls
            An authenticated session that is used in all API calls. Must supply a
            valid bearer token to all API calls.
        cache : bool
            Enable caching (default).
        cache_opt : dict, optional
            Configuration object for controlling the series cache.
            'max_size': max size of cache in megabytes. Default is 1024 MB.
            'cache_root': cache storage location. See documentation for platform
            specific defaults.

        """
        if cache:
            cache_opt = {} if cache_opt is None else cache_opt
            storage_cache = StorageCache(**cache_opt)
        else:
            storage_cache = None

        self._timeseries_api = timeseries_api
        self._downloader = BaseDownloader(storage_cache)
        self._session = session

    def put(self, df, target_url, commit_request):
        """
        Put a Pandas DataFrame into storage.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to store.
        target_url : str
            Blob storage URL.
        commit_request : tuple
            Parameteres for "commit" request. Given as `(METHOD, URL, kwargs)`.
            The tuple is passed forward to `session.request(METHOD, URL, **kwargs)`

        Returns
        -------
            The unique file id as stored in the reservoir

        """
        _df_to_blob(df, target_url)

        method, url, kwargs = commit_request
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return

    def get(self, timeseries_id, start, end):
        # (target_url | start and end already resolved in the url)
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
        # make REST call to timeseries/days
        # Unpack and put blob urls in sequence
        # download 1-by-1 and update using dict
        # return final merged.

        # No extra logic, just download and merge in sequence.
        # Optimal use: target_url covers only 1 day.

        log.debug("getting day file inventory")
        response = self._timeseries_api.download_days(timeseries_id, start, end)
        df = self._downloader.get(response)
        return df

# dict merge function.

class BaseDownloader:
    """
    Series download strategy that will download Pandas DataFrame from provided
    cloud backend.

    Multiple chunks will be downloaded in parallel using ThreadPoolExecutor.

    """

    def __init__(self, cacheio=None):
        self._cacheio = cacheio

    def get(self, response):
        filechunks = [f["Chunks"] for f in response["Files"]]
        filedatas = map(self._download_chunks_as_dataframe, filechunks)

        try:
            df = next(filedatas)
        except StopIteration:
            return pd.DataFrame(columns=("index", "values")).astype({"index": "int64"})

        for fd in filedatas:
            df = self._combine_first(fd, df)

        df.reset_index(inplace=True)  # Temporary hotfix while waiting for refactor
        return df

    def _download_chunks_as_dataframe(self, chunks):
        if not chunks:
            df_chunks = pd.DataFrame(columns=("index", "values")).astype(
                {"index": "int64"}
            )
            df_chunks.set_index(
                "index", inplace=True
            )  # Temporary hotfix while waiting for refactor
            return df_chunks

        with ThreadPoolExecutor(max_workers=1) as executor:
            filechunks = executor.map(self._download_verified_chunk, chunks)
        df_chunks = pd.concat(filechunks)
        return df_chunks

    def _download_verified_chunk(self, chunk):
        """
        Download chunk as Pandas DataFrame and ensure the series does not contain
        duplicates.
        """
        if self._cacheio is not None:
            df = self._cacheio.get(chunk)

            if df is None:
                blob_url = chunk["Endpoint"]
                df = _blob_to_df(blob_url)
                self._cacheio.put(df, chunk)
        else:
            blob_url = chunk["Endpoint"]
            df = _blob_to_df(blob_url)

        df.set_index(
            "index", inplace=True
        )  # Temporary hotfix while waiting for refactor
        if not df.index.is_unique:
            return df[~df.index.duplicated(keep="last")]
        return df

    @staticmethod
    def _combine_first(calling, other):
        """
        Faster combine first for most common scenarios and fall back to general
        purpose Pandas combine_first for advanced cases.
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


class StorageCache(CacheIO):
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
    """

    STOREFORMATVERSION = "v3"
    CACHE_THRESHOLD = 24 * 60  # number of rows

    def __init__(self, max_size=1024, cache_root=None, cache_folder="datareservoirio"):
        self._max_size = max_size * 1024 * 1024
        self._cache_format = "parquet"

        self._init_cache_dir(cache_root, cache_folder)
        self._cache_index = _CacheIndex(self._cache_path, self._max_size)

        self._evict_lock = Lock()
        self._evict_from_cache()

        super().__init__()

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
        else:
            log.debug(f"Cache hit on {id_}")
        return data

    def _get_cache_id_md5(self, chunk):
        path = chunk["Path"]
        md5 = _encode_for_path_safety(chunk["ContentMd5"])
        id_ = re.sub(r"-|_|/|\.", "", path)
        return self._cache_format + id_, md5  # modify id_ with format prefix

    def put(self, data, chunk):
        id_, md5 = self._get_cache_id_md5(chunk)
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


def _blob_to_df(blob_url):
    """
    Download blob from remote storage and present as a Pandas Series.

    Parameters
    ----------
    blob_url : str
        Fully formated URL to the blob. Must contain all the required parameters
        in the URL.

    Return
    ------
    series : pandas.Series
        Pandas series where index is nano-seconds since epoch and values are ``str``
        or ``float64``.
    """

    response = requests.get(blob_url, stream=True)
    response.raise_for_status()

    with io.BytesIO() as stream:
        for chunk in response.iter_content(chunk_size=512):
            stream.write(chunk)

        stream.seek(0)
        df = pd.read_csv(
            stream,
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "str"},
            encoding="utf-8",
        ).astype({"values": "float64"}, errors="ignore")

    return df


def _df_to_blob(df, blob_url):
    """
    Upload a Pandas Dataframe as blob to a remote storage.

    The series object converted to CSV encoded in "utf-8". Headers are ignored
    and line terminator is set as ``\n``.

    Parameters
    ----------
    series : pandas.Series
        Pandas series where index is nano-seconds since epoch (``Int64``) and
        values are ``str`` or ``float64``.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError

    with io.BytesIO() as fp:
        kwargs = {"header": False, "index": False, "encoding": "utf-8", "mode": "wb"}
        try:  # breaking change since pandas 1.5.0
            df.to_csv(fp, lineterminator="\n", **kwargs)
        except TypeError:  # Compatibility with pandas older than 1.5.0.
            df.to_csv(fp, line_terminator="\n", **kwargs)
        fp.seek(0)

        requests.put(
            blob_url, headers={"x-ms-blob-type": "BlockBlob"}, data=fp
        ).raise_for_status()
    return
