import logging
import time
import timeit
import warnings
from operator import itemgetter

import pandas as pd
import requests

from .globalsettings import environment
from .rest_api import FilesAPI, MetadataAPI, TimeSeriesAPI
from .storage import BaseDownloader, DirectDownload, FileCacheDownload, Storage

log = logging.getLogger(__name__)


# Default values to push as start/end dates. (Limited by numpy.datetime64)
_END_DEFAULT = 9214646400000000000  # 2262-01-01
_START_DEFAULT = -9214560000000000000  # 1678-01-01


class Client:
    """
    DataReservoir.io client for user-friendly interaction.

    Parameters
    ---------
    auth : cls
        An authenticated session that is used in all API calls. Must supply a
        valid bearer token to all API calls.
    cache : bool
        Enable caching (default).
    cache_opt : dict, optional
        Configuration object for controlling the series cache.
        'format': 'parquet' or 'csv'. Default is 'parquet'.
        'max_size': max size of cache in megabytes. Default is 1024 MB.
        'cache_root': cache storage location. See documentation for platform
        specific defaults.

    """

    CACHE_DEFAULT = {"format": "parquet", "max_size": 1024, "cache_root": None}

    def __init__(self, auth, cache=True, cache_opt=CACHE_DEFAULT):
        self._auth_session = auth
        self._timeseries_api = TimeSeriesAPI(self._auth_session, cache=cache)
        self._files_api = FilesAPI(self._auth_session)
        self._metadata_api = MetadataAPI(self._auth_session)
        self._enable_cache = cache

        if self._enable_cache:
            cache_default = self.CACHE_DEFAULT.copy()
            if set(cache_default.keys()).issuperset(cache_opt):
                cache_default.update(cache_opt)
                self._cache_opt = cache_default
                self._cache_format = self._cache_opt.pop("format")
            else:
                raise ValueError("cache_opt contains unknown keywords.")

        if self._enable_cache:
            download_backend = FileCacheDownload(
                format_=self._cache_format, **self._cache_opt
            )
        else:
            download_backend = DirectDownload()

        downloader = BaseDownloader(download_backend)

        self._storage = Storage(self._timeseries_api, downloader, self._auth_session)

    def ping(self):
        """
        Test that you have a working connection to DataReservoir.io.

        """
        return self._files_api.ping()

    def create(self, series=None, wait_on_verification=True):
        """
        Create a new series in DataReservoir.io from a pandas.Series. If no
        data is provided, an empty series is created.

        Parameters
        ----------
        series : pandas.Series, optional
            Series with index (as DatetimeIndex-like or integer array). Default
            is None.
        wait_on_verification : bool (optional)
            All series are subjected to a server-side data validation before
            they are made available for consumption; failing validation will
            result in the series being ignored. If True, the method will wait
            for the data validation process to be completed and return the
            outcome, which may be time consuming. If False, the method will NOT
            wait for the outcome and the data will be available when/if the
            validation is successful. The latter is significantly faster, but
            is recommended when the data is "validated" in advance.
            Default is True.

        Returns
        -------
        dict
            The response from DataReservoir.io containing the unique id of the
            newly created series.
        """
        if series is None:
            response = self._timeseries_api.create()
            return response

        df = self._verify_and_prepare_series(series)

        response_file = self._auth_session.post(
            environment.api_base_url + "files/upload"
        )
        response_file.raise_for_status()
        file_id, target_url = itemgetter("FileId", "Endpoint")(response_file.json())

        commit_request = (
            "POST",
            environment.api_base_url + "files/commit",
            {"json": {"FileId": file_id}},
        )
        self._storage.put(df, target_url, commit_request)

        if wait_on_verification:
            status = self._wait_until_file_ready(file_id)
            if status == "Failed":
                return status

        response = self._timeseries_api.create_with_data(file_id)
        return response

    def append(self, series, series_id, wait_on_verification=True):
        """
        Append data to an already existing series.

        Parameters
        ----------
        series : pandas.Series
            Series with index (as DatetimeIndex-like or integer array).
        series_id : string
            The identifier of the existing series.
        wait_on_verification : bool (optional)
            All series are subjected to a server-side data validation before
            they are made available for consumption; failing validation will
            result in the series being ignored. If True, the method will wait
            for the data validation process to be completed and return the
            outcome, which may be time consuming. If False, the method will NOT
            wait for the outcome and the data will be available when/if the
            validation is successful. The latter is significantly faster, but
            is recommended when the data is "validated" in advance.
            Default is True.

        Returns
        -------
        dict
            The response from DataReservoir.io.
        """
        df = self._verify_and_prepare_series(series)

        response_file = self._auth_session.post(
            environment.api_base_url + "files/upload"
        )
        response_file.raise_for_status()
        file_id, target_url = itemgetter("FileId", "Endpoint")(response_file.json())

        commit_request = (
            "POST",
            environment.api_base_url + "files/commit",
            {"json": {"FileId": file_id}},
        )

        self._storage.put(df, target_url, commit_request)

        if wait_on_verification:
            status = self._wait_until_file_ready(file_id)
            if status == "Failed":
                return status

        response = self._timeseries_api.add(series_id, file_id)
        return response

    def info(self, series_id):
        """
        Retrieve basic information about a series.

        Returns
        -------
        dict
            Available information about the series. None if series not found.
        """
        return self._timeseries_api.info(series_id)

    def search(self, namespace, key=None, name=None, value=None):
        """
        Find available series having metadata with given
        namespace + key* (optional) + name (optional) + *value* (optional)
        combination. Note that the arguments are hierarchical, starting from
        the left. If an argument is None, the proceeding ones are also set to
        None. For example, (namespace = “hello”, key=None, name=”Rabbit”, value=”Hole”)
        will have the same effect as (namespace = “hello”, key=None, name=None,
        value=None)

        Parameters
        ----------
        namespace : str
            Full namespace to search for
        key : str, optional
            Key or partial (begins with) key to narrow search.
            Default (None) will include all.
        name: str, optional
            Full name to narrow search further.
            Default (None) will include all.
        value: str, optional
            Value or partial (begins or ends with or both) to narrow search further.
            Default (None) will include all.

        Returns
        -------
        dict or list
            Available information about the series. If ``value`` is passed,
            a plain list with ``TimeSeriesId`` is returned. Otherwise, a dict is
            returned -> ``{TimeSeriesId: metadata}``.

        """
        args_ = [namespace, key, name, value]
        none_count = args_.count(None)

        if args_[-none_count:].count(None) < none_count:
            warnings.warn(
                "Warning: You have provided argument(s) following a None argument, they are ignored by the search!"
            )

        return self._timeseries_api.search(namespace, key, name, value)

    def delete(self, series_id):
        """
        Delete a series from DataReservoir.io.

        Parameters
        ----------
        series_id : string
            The id of the series to delete.

        """
        return self._timeseries_api.delete(series_id)

    def get(
        self,
        series_id,
        start=None,
        end=None,
        convert_date=True,
        raise_empty=False,
    ):
        """
        Retrieve a series from DataReservoir.io.

        Parameters
        ----------
        series_id : str
            Identifier of the series to download
        start : optional
            start time (inclusive) of the series given as anything
            pandas.to_datetime is able to parse.
        end : optional
            stop time (exclusive) of the series given as anything
            pandas.to_datetime is able to parse.
        convert_date : bool
            If True (default), the index is converted to DatetimeIndex.
            If False, index is returned as ascending integers.
        raise_empty : bool
            If True, raise ValueError if no data exist in the provided
            interval. Otherwise, return an empty pandas.Series (default).

        Returns
        -------
        pandas.Series
            Series data
        """
        if not start:
            start = _START_DEFAULT
        if not end:
            end = _END_DEFAULT

        start = pd.to_datetime(start, dayfirst=True, unit="ns", utc=True).value
        end = pd.to_datetime(end, dayfirst=True, unit="ns", utc=True).value - 1

        if start >= end:
            raise ValueError("start must be before end")

        time_start = timeit.default_timer()

        log.debug("Getting series range")
        series = (
            self._storage.get(series_id, start, end)
            .set_index("index")
            .squeeze("columns")
            .loc[start:end]
            .copy(deep=True)
        )
        series.index.name = None

        if series.empty and raise_empty:  # may become empty after slicing
            raise ValueError("can't find data in the given interval")

        if convert_date:
            series.index = pd.to_datetime(series.index, utc=True)

        time_end = timeit.default_timer()
        log.info(f"Download series dataframe took {time_end - time_start} seconds")

        return series

    def set_metadata(
        self,
        series_id,
        metadata_id=None,
        namespace=None,
        key=None,
        overwrite=False,
        **namevalues,
    ):
        """
        Set metadata entries on a series. Metadata can be set from existing
        values or new metadata can be created.

        Parameters
        ----------
        series_id : str
            The identifier of the existing series
        metadata_id : str, optional
            The identifier of the existing metadata entries. If passed, other
            metadata related arguments are ignored.
        namespace : str, optional
            Metadata namespace.
        key : str, mandatory if namespace is passed.
            Metadata key.
        overwrite: bool, optional
            If true, and namespace+key corresponds to existing metadata, the
            value of the metadata will be overwritten. If false, a ValueError
            will be raised if the metadata already exist.
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        if not metadata_id and not namespace:
            raise ValueError("one of metadata_id or namespace is mandatory")
        elif not metadata_id and namespace:
            if not key:
                raise ValueError("key is mandatory when namespace is passed")
            try:
                response_create = self._metadata_api.put(
                    namespace, key, overwrite, **namevalues
                )
                metadata_id = response_create["Id"]
            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 409:
                    raise ValueError(
                        "Metadata already exist. Specify overwrite=True to"
                        "confirm overwriting the metadata."
                    )
                else:
                    raise

        response = self._timeseries_api.attach_metadata(series_id, [metadata_id])
        return response

    def remove_metadata(self, series_id, metadata_id):
        """
        Remove a metadata entry from a series. Note that metadata entries are
        not deleted, but the link between series and metadata is broken.

        Parameters
        ----------
        series_id : str
            The identifier of the existing series
        metadata_id : str
            The identifier of the existing metadata entry.

        Return
        ------
        dict
            response.json()
        """
        response = self._timeseries_api.detach_metadata(series_id, [metadata_id])
        return response

    def metadata_set(self, namespace, key, **namevalues):
        """
        Create or update a metadata entry. If the namespace/key combination
        does not already exist, a new entry will be created. If the combination
        already exist, the entry will be updated with the specified namevalues.

        Parameters
        ----------
        namespace : str
            Metadata namespace
        key : str
            Metadata key
        namevalues : keyword arguments
            Metadata name-value pairs

        Returns
        -------
        dict
            The response from DataReservoir.io containing the unique id of the
            new or updated metadata.
        """
        response = self._metadata_api.put(namespace, key, True, **namevalues)
        return response

    def metadata_get(self, metadata_id=None, namespace=None, key=None):
        """
        Retrieve a metadata entry. Required input is either metatdata_id, or
        namespace + key, i.e. metadata_get(my_metadata_id) or metadata_get(my_namespace, my_key)

        Parameters
        ----------
        metadata_id : str
            The identifier of existing metadata
        namespace : str
            Metadata namespace. Ignored if metadata_id is set.
        key : str
            Metadata key. Ignored if metadata_id is set.

        Returns
        -------
        dict
            Metadata entry.
        """
        if metadata_id:
            response = self._metadata_api.get_by_id(metadata_id)
        elif namespace and key:
            response = self._metadata_api.get(namespace, key)
        else:
            raise ValueError(
                "Missing required input: either (metadata_id) or (namespace, key)"
            )

        return response

    def metadata_browse(self, namespace=None):
        """
        List available metadata namespaces and keys. If namespace is None, a list
        of all available namespaces is returned. If namespace is specified,
        a list of all available keys for that namespace is returned.

        Parameters
        ----------
        namespace : string
            The namespace to search in (exact match)

        Returns
        -------
        list
            The namespaces or keys found.
        """

        if not namespace:
            return self._metadata_api.namespaces()
        else:
            return self._metadata_api.keys(namespace)

    def metadata_search(self, namespace, key):
        """
        Find metadata entries given namespace/key combination.

        namespace : string
            The namespace to search in
        key : string
            The key to narrow search. Supports "begins with" specification,
            i.e. will look for matches with "key + wildcard"

        Returns
        -------
        list
            Metadata entries that matches the search.

        """
        response = self._metadata_api.search(namespace, key)
        return response

    def metadata_delete(self, metadata_id):
        """
        Delete an existing metadata entry.

        Parameters
        ----------
        metadata_id : str
            id of metadata
        """
        self._metadata_api.delete(metadata_id)
        return

    def _verify_and_prepare_series(self, series):
        if not isinstance(series, pd.Series):
            raise ValueError("series must be a pandas Series")

        if not (
            pd.api.types.is_datetime64_ns_dtype(series.index)
            or pd.api.types.is_int64_dtype(series.index)
        ):
            raise ValueError("allowed dtypes are datetime64[ns] and int64")

        if not series.index.is_unique:
            raise ValueError("index values must be unique timestamps")

        df = series.to_frame(name=1).reset_index(names=0)
        df[0] = df[0].view("int64")

        return df

    def _wait_until_file_ready(self, file_id):
        # wait for server side processing
        while True:
            status = self._get_file_status(file_id)
            log.debug(f"status is {status}")

            if status == "Ready":
                return "Ready"
            elif status == "Failed":
                return "Failed"

            time.sleep(5)

    def _get_file_status(self, file_id):
        response = self._files_api.status(file_id)
        return response["State"]
