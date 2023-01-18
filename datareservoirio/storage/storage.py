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

        df = series.to_frame(name="values")
        df.index = df.index.view("int64")

        self._uploader.put(upload_params, df)

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

        series = (
            self._downloader.get(response)
            .squeeze("columns")
            .loc[start:end]
            .copy(deep=True)
        )
        return series


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

        with ThreadPoolExecutor(max_workers=1) as executor:
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


class FileCacheDownload(CacheIO):
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
            blob_url = chunk["Endpoint"]
            data = _blob_to_df(blob_url)
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


class DirectDownload:

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
        blob_url = chunk["Endpoint"]
        return _blob_to_df(blob_url)


class DirectUpload:

    """
    Backend for direct upload to cloud.

    """

    def put(self, params, data):
        blob_url = params["Endpoint"]
        return _df_to_blob(data, blob_url)


def _blob_to_df(blob_url):
    """
    Download blob from remote storage and present as a Pandas Series.

    Parameters
    ----------
    blob_url : str
        Fully formated URL to the blob. Must contail all the required parameters
        in the URL.

    Return
    ------
    series : pandas.Series
        Pandas series where index is nano-seconds since epoch and values are ``str``
        or ``float64``.
    """
    df = pd.read_csv(
        blob_url,
        header=None,
        names=("values",),
        dtype={0: "int64", 1: "str"},
        index_col=0,
        encoding="utf-8",
    ).astype("float64", errors="ignore")
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
        kwargs = {"header": False, "encoding": "utf-8", "mode": "wb"}
        try:  # breaking change since pandas 1.5.0
            df.to_csv(fp, lineterminator="\n", **kwargs)
        except TypeError:  # Compatibility with pandas older than 1.5.0.
            df.to_csv(fp, line_terminator="\n", **kwargs)
        fp.seek(0)

        requests.put(
            blob_url, headers={"x-ms-blob-type": "BlockBlob"}, data=fp
        ).raise_for_status()
    return
