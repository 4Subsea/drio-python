from __future__ import absolute_import, division, print_function

import logging
import time
import timeit
import requests
import pandas as pd

from .log import LogWriter
from .rest_api import FilesAPI, TimeSeriesAPI, MetadataAPI
from .storage import (AlwaysDownloadStrategy, CachedDownloadStrategy,
                      SimpleFileCache, Storage, UploadStrategy)

logger = logging.getLogger(__name__)
log = LogWriter(logger)

# Default values to push as start/end dates. (Limited by numpy.datetime64)
_END_DEFAULT = 9214646400000000000  # 2262-01-01
_START_DEFAULT = -9214560000000000000  # 1678-01-01


class Client(object):
    """
    Client handles requests, data uploads, and data downloads from the 4Subsea
    data reservoir.

    Parameters
    ---------
    auth : cls
        An authenticator class with :attr:`token` attribute that provides a
        valid token to the 4Subsea data reservoir.
    cache : bool
        Enable caching (default).
    cache_opt : dict, optional
        Configuration object for controlling the timeseries cache.
        'format': 'msgpack' or 'csv'. Default is 'msgpack'.
        'max_size': max size of cache in megabytes. Default is 1024 MB.
        'cache_root': cache storage location, defaults to
        %LOCALAPPDATA%\\reservoir_cache in Windows, or similar in other
        platforms.

    """
    CACHE_DEFAULT = {'format': 'msgpack', 'max_size': 1024, 'cache_root': None}

    def __init__(self, auth, cache=True, cache_opt=CACHE_DEFAULT):
        self._session = requests.Session()
        self._authenticator = auth
        self._timeseries_api = TimeSeriesAPI(cache=cache, session=self._session)
        self._files_api = FilesAPI(session=self._session)
        self._metadata_api = MetadataAPI(session=self._session)
        self._enable_cache = cache

        if self._enable_cache:
            cache_default = self.CACHE_DEFAULT.copy()
            if set(cache_default.keys()).issuperset(cache_opt):
                cache_default.update(cache_opt)
                self._cache_opt = cache_default
                self._cache_format = self._cache_opt.pop('format')
            else:
                raise ValueError('cache_opt contains unknown keywords.')

        if self._enable_cache:
            downloader = CachedDownloadStrategy(SimpleFileCache(**self._cache_opt),
                                                format=self._cache_format,
                                                session=self._session)
        else:
            downloader = AlwaysDownloadStrategy(session=self._session)

        uploader = UploadStrategy(session=self._session)

        self._storage = Storage(
            self._authenticator,
            self._timeseries_api,
            self._files_api,
            downloader=downloader,
            uploader=uploader)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._session.close()

    @property
    def token(self):
        """
        Your token that is sent to the data reservoir with every request you
        make. There is no password stored in the token, it only provides access
        for a limited amount of time and only to the data reservoir.
        """
        return self._authenticator.token

    def ping(self):
        """
        With ping you can test that you have a working connection to the data
        reservoir.
        """
        return self._files_api.ping(self.token)

    def create(self, series=None):
        """
        Create a new series in the reservoir from a pandas.Series. If no data
        is provided, an empty series is created.

        Parameters
        ----------
        series : pandas.Series, optional
            Series with index (as numpy.datetime64 (with nanosecond precision)
            or integer array). Default is None.

        Returns
        -------
        dict
            The response from the reservoir containing the unique id of the
            newly created series.
        """
        if series is None:
            response = self._timeseries_api.create(self.token)
            return response

        self._verify_and_prepare_series(series)

        time_start = timeit.default_timer()
        file_id = self._storage.put(series)
        time_upload = timeit.default_timer()
        log.info('Upload took {} seconds'
                 .format(time_upload - time_start), 'create')

        status = self._wait_until_file_ready(file_id)
        time_process = timeit.default_timer()
        log.info('Processing took {} seconds'
                 .format(time_process - time_upload), 'create')
        if status == "Failed":
            return status

        response = self._timeseries_api.create_with_data(self.token, file_id)
        time_end = timeit.default_timer()
        log.info('Done. Total time spent: {} seconds ({} minutes)'
                 .format(time_end - time_start, (time_end - time_start) / 60.), 'create')
        return response

    def append(self, series, series_id):
        """
        Append data to an already existing series.

        Parameters
        ----------
        series : pandas.Series
            Series with index (as numpy.datetime64 (with nanosecond precision)
            or integer array).
        series_id : string
            the identifier of the existing series.

        Returns
        -------
        dict
            The response from the reservoir
        """
        self._verify_and_prepare_series(series)

        time_start = timeit.default_timer()
        file_id = self._storage.put(series)
        time_upload = timeit.default_timer()
        log.info('Upload took {} seconds'
                 .format(time_upload - time_start), 'append')

        status = self._wait_until_file_ready(file_id)
        time_process = timeit.default_timer()
        log.info('Processing serverside took {} seconds'
                 .format(time_process - time_upload), 'append')
        if status == "Failed":
            return status

        time_end = timeit.default_timer()
        log.info('Done, total time spent: {} seconds ({} minutes)'
                 .format(time_end - time_start, (time_end - time_start) / 60.), 'append')

        response = self._timeseries_api.add(self.token, series_id, file_id)
        return response

    def info(self, series_id):
        """
        Retrieves basic information about one timeseries.

        Returns
        -------
        dict
            Available information about the timeseries. None if timeseries not
            found.
        """
        return self._timeseries_api.info(self.token, series_id)

    def search(self, namespace, key, name, value=None):
        """
        Find available series having metadata with given
        namespace/key/name/value combination.

        Parameters
        ----------
        namespace : str
            The namespace to search in
        key : str
            The key to narrow search
        name: str
            name to narrow search further
        value: str, optional
            Value to narrow search further. Default (None) will include all.

        Returns
        -------
        dict or list
            Available information about the timeseries. If ``value`` is passed,
            a dict is returned -> ``{timeseries_id: value}``. Otherwise, a
            plain list with timeseries_id is returned.
        """
        return self._timeseries_api.search(self.token, namespace, key, name,
                                           value)

    def delete(self, series_id):
        """
        Parameters
        ----------
        timeseries_id : string
            The id of the timeseries to delete.

        Returns
        -------
        int
            http status code. 200 is OK
        """
        return self._timeseries_api.delete(self.token, series_id)

    def get(self, series_id, start=None, end=None, convert_date=True,
            raise_empty=False):
        """
        Retrieves a timeseries from the data reservoir.

        Parameters
        ----------
        series_id : str
            id of the timeseries to download
        start : optional
            start time (inclusive) of the timeseries given as anything
            pandas.to_datetime is able to parse.
        end : optional
            stop time (inclusive) of the timeseries given as anything
            pandas.to_datetime is able to parse.
        convert_date : bool
            If True (default), the index is converted to numpy.datetime64[ns]. If
            False, index is returned as nanoseconds since epoch.
        raise_empty : bool
            If True, raise ValueError if no data exist in the provided
            interval. Otherwise, return an empty pandas.Series (default).

        Returns
        -------
        pandas.Series
            Timeseries data
        """
        if not start:
            start = _START_DEFAULT
        if not end:
            end = _END_DEFAULT

        start = pd.to_datetime(start, dayfirst=True, unit='ns').value
        end = pd.to_datetime(end, dayfirst=True, unit='ns').value

        if start >= end:
            raise ValueError('start must be before end')

        time_start = timeit.default_timer()

        log.debug("Getting timeseries range")
        series = self._storage.get(series_id, start, end)

        if series.empty and raise_empty:  # may become empty after slicing
            raise ValueError('can\'t find data in the given interval')

        if convert_date:
            series.index = pd.to_datetime(series.index)

        time_end = timeit.default_timer()
        log.info('Download timeseries dataframe took {} seconds'
                 .format(time_end - time_start), 'get')

        return series

    def set_metadata(self, series_id, metadata_id=None, namespace=None,
                     key=None, overwrite=False, **namevalues):
        """
        Set metadata entries on a series. Metadata can be set from existing values
        or new metadata can be created.

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
            If true, and namespace+key corresponds to existing metadata, the value of the
            metadata will be overwritten. If false, a ValueError will be raised if the
            metadata already exist.
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        if not metadata_id and not namespace:
            raise ValueError('one of metadata_id or namespace is mandatory')
        elif not metadata_id and namespace:
            if not key:
                raise ValueError('key is mandatory when namespace is passed')
            try:
                response_create = self._metadata_api.put(
                    self.token, namespace, key, overwrite, **namevalues)
                metadata_id = response_create['Id']
            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 409:
                    raise ValueError('Metadata already exist. Specify overwrite=True to confirm overwriting the metadata')
                else:
                    raise

        response = self._timeseries_api.attach_metadata(
            self.token, series_id, [metadata_id])
        return response

    def remove_metadata(self, series_id, metadata_id):
        """
        Remove a metadata entry from a series. Note that metadata entries are
        not deleted, but the link between series and metadata is broken.

        Parameters
        ----------
        series_id : str
            The identifier of the existing series
        metadata_ids : str
            The identifier of the existing metadata entry.

        Return
        ------
        dict
            response.json()
        """
        response = self._timeseries_api.detach_metadata(
            self.token, series_id, [metadata_id])
        return response

    def metadata_set(self, namespace, key, **namevalues):
        """
        Create or update a metadata entry. If the namespace/key combination does not already
        exist, a new entry will be created. If the combination already exist, the entry
        will be updated with the specified namevalues.

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
            The response from datareservoir.io containing the unique id of the
            new or updated metadata.
        """
        response = self._metadata_api.put(
            self.token, namespace, key, True, **namevalues)
        return response

    def metadata_get(self, metadata_id=None, namespace=None, key=None):
        """
        Retrieve a metedata entry.

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
            response = self._metadata_api.get_by_id(self.token, metadata_id)
        elif namespace and key:
            response = self._metadata_api.get(self.token, namespace, key)
        else:
            raise ValueError('key is mandatory when namespace is passed')

        return response

    def metadata_browse(self, namespace=None, key=None):
        """
        Browse available metadata namespace/key/names combinations.

        Parameters
        ----------
        namespace : string
            The namespace to search in
        key : string
            the namespace key to narrow search

        Returns
        -------
        list
            The namespaces or keys found. Or if both namespace and key is present, the
            specific metadata for the namespace/key combination
        """

        if not namespace:
            return self._metadata_api.namespaces(self.token)
        elif not key:
            return self._metadata_api.keys(self.token, namespace)
        else:
            response = self._metadata_api.get(self.token, namespace, key)
            return response['Value']

    def metadata_search(self, namespace, key, conjunctive=True):
        """
        Find metadata entries given namespace/key/name-value combination.

        namespace : string
            The namespace to search in
        key : string
            The key to narrow search
        namevalues : keyword arguments
            Name/value pairs to narrow search further

        Returns
        -------
        dict
            Metadata entries that matches the search.
        """
        response = self._metadata_api.search(self.token, namespace, key,
                                             conjunctive)
        return response

    def _verify_and_prepare_series(self, series):
        log.debug("checking arguments", "_check_arguments_create")

        if not isinstance(series, pd.Series):
            log.error("series type is {}".format(type(series)))
            raise ValueError('series must be a pandas Series')

        if not (pd.api.types.is_datetime64_ns_dtype(series.index) or
                pd.api.types.is_int64_dtype(series.index)):
            log.error("index dtype is {}".format(series.index.dtype))
            raise ValueError('allowed dtypes are datetime64[ns] and int64')

        if not series.index.is_unique:
            log.error("index contains duplicate timestamp values")
            raise ValueError('index values must be unique timestamps')

    def _wait_until_file_ready(self, file_id):
        # wait for server side processing
        while True:
            status = self._get_file_status(file_id)
            log.debug("status is {}".format(status), "create")

            if status == "Ready":
                return "Ready"
            elif status == "Failed":
                return "Failed"

            time.sleep(5)

    def _get_file_status(self, file_id):
        response = self._files_api.status(self.token, file_id)
        return response['State']
