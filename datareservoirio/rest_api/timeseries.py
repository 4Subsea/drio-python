import logging
from datetime import datetime, timedelta
from functools import update_wrapper
from threading import RLock as Lock
from uuid import uuid4

from .base import BaseAPI

log = logging.getLogger(__name__)


def request_cache(timeout=180):
    """
    Cache request response with timeout.

    Assumes that first positional argument is token and is not included in the
    request cache signature.
    """

    def func_wrapper(func):
        wrapper = _request_cache_wrapper(func, timeout, maxsize=256)
        return update_wrapper(wrapper, func)

    return func_wrapper


def _request_cache_wrapper(func, timeout, maxsize, skip_argpos=1):
    cache_lock = Lock()  # lock used to protect cache during modifications
    sentinel = object()  # unique object used to signal cache misses
    cache = {}

    def wrapper(*args, **kwargs):
        timestamp = datetime.utcnow()

        request_signature = _make_request_hash(args[skip_argpos:], kwargs)

        log.debug("attempting to access request cache")
        result, timestamp_cache = cache.get(request_signature, (sentinel, None))

        if result is sentinel or (
            timestamp_cache + timedelta(seconds=timeout) < timestamp
        ):
            log.debug("request cache invalid - acquiring from source")
            result = func(*args, **kwargs)

            with cache_lock:  # altering cache is now thread-safe
                cache[request_signature] = (result, timestamp)
                if len(cache) > maxsize:
                    log.debug("request cache full - pop out oldest item")
                    key = sorted(cache, key=lambda x: cache.get(x)[-1])[0]
                    del cache[key]
        return result

    wrapper._cache = cache
    return wrapper


def _make_request_hash(args, kwargs):
    """Hash request signature."""
    key = args
    for kwarg in kwargs.items():
        key += kwarg
    return hash(key)


class TimeSeriesAPI(BaseAPI):
    """
    Python wrapper for reservoir-api.4subsea.net/api/timeseries.

    Parameters
    ----------
    session :
        Authorized session instance (User or Client) which appends a valid bearer token to all
        HTTP calls.
    cache : bool
        Whether to cache calls and responses with expiry time. Defaults to
        True.

    """

    def __init__(self, session, cache=True):
        super(TimeSeriesAPI, self).__init__(session=session)
        self._root = self._api_base_url + "timeseries/"
        self._cache = cache

    def create(self, timeseries_id=None):
        """
        Create timeseries entry.

        Parameters
        ----------
        timeseries_id : str or None
            id of the timeseries (Timeseries API) to create.
            if None, a unique id will be generated.

        Return
        ------
        dict
            http response.text loaded as json
        """
        log.debug(f"create with <token>, {timeseries_id}")
        if timeseries_id is None:
            timeseries_id = str(uuid4())

        uri = self._root + timeseries_id
        response = self._put(uri, data=None)
        return response.json()

    def create_with_data(self, file_id=None):
        """
        Create timeseries entry with data from a file.

        Parameters
        ----------
        file_id : str
            id of file (Files API) to be added to the timeseries.

        Return
        ------
        dict
            http response.text loaded as json
        """
        log.debug(f"create with <token>, {file_id}")

        uri = self._root + "create"
        body = {"FileId": file_id}
        response = self._post(uri, data=body)
        return response.json()

    def add(self, timeseries_id, file_id):
        """
        Append timeseries data to an existing entry.

        Parameters
        ----------
        timeseries_id : str
            id of the target timeseries
        file_id : str
            id of file (File API) to be appended.

        Notes
        -----
        Refer to API documentation wrt append, overlap, and overwrite behavior
        """
        log.debug(f"add with <token>, {timeseries_id}, {file_id}")

        uri = self._root + "add"
        body = {"TimeSeriesId": timeseries_id, "FileId": file_id}
        response = self._post(uri, data=body)
        return response.json()

    def info(self, timeseries_id):
        """
         Information about a timeseries entry.

         Parameters
         ----------
        timeseries_id : str
             id of the target timeseries

         Return
         ------
         dict
             dictionary containing information about a timeseries
             entry in the reservoir
        """
        log.debug(f"info with <token>, {timeseries_id}")

        uri = self._root + timeseries_id
        response = self._get(uri)
        return response.json()

    def delete(self, timeseries_id):
        """
        Delete a timeseries.

        Parameters
        ----------
        timeseries_id : str
            id of the target timeseries
        """
        log.debug(f"delete with <token>, {timeseries_id}")

        uri = self._root + timeseries_id
        self._delete(uri)
        return

    def download_days(self, timeseries_id, start, end):
        """
        Return timeseries data with start/stop.

        Parameters
        ----------
        timeseries_id : str
            id of the timeseries to download
        start : int long
            start time in nano seconds since epoch.
        end : int long
            end time in nano seconds since epoch.

        Return
        ------
        json
            a list of files, each having a list of Azure Storage compatible
            chunk urls

        Note
        ----
        Requests are divided into n-sub requests per day.
        """
        log.debug(f"download_days with <token>, {timeseries_id}, {start}, {end}")

        nanoseconds_day = 86400000000000
        start = (start // nanoseconds_day) * nanoseconds_day
        end = ((end // nanoseconds_day) + 1) * nanoseconds_day - 1

        download_days = (
            self._download_days_cached if self._cache else self._download_days_base
        )
        return download_days(timeseries_id, start, end)

    @request_cache(timeout=180)
    def _download_days_cached(self, *args):
        return self._download_days_base(*args)

    def _download_days_base(self, timeseries_id, start, end):
        """
        Return timeseries data with start/stop.
        Internal low level function to allow for higher level operations on
        public counterpart.

        Parameters
        ----------
        timeseries_id : str
            id of the timeseries to download
        start : int long
            start time in nano seconds since epoch.
        end : int long
            end time in nano seconds since epoch.

        Return
        ------
        json
            a list of files, each having a list of Azure Storage compatible chunk urls
        """
        log.debug(f"download_days_base with <token>, {timeseries_id}, {start}, {end}")

        uri = self._root + "{}/data/days".format(timeseries_id)
        params = {"start": start, "end": end}

        response = self._get(uri, params=params)
        return response.json()

    def search(self, namespace, key=None, name=None, value=None):
        """
        Find timeseries with metadata for given namespace/key/name/value
        combination.

        Parameters
        ----------
        namespace : str
            namespace in metadata
        key : str, optional
            key in metadata
        name : str, optional
            name in name/value-pair found in metadata value-json
        value : str, optional
            value in name/value-pair found in metadata value-json

        Return
        ======
        dict or list
            response.json() containing timeseriesID

        """
        args_update = []
        for arg in [namespace, key, name, value]:
            if not arg:
                break
            else:
                args_update.append(arg)

        uri = self._root + "search/" + "/".join(args_update)
        response = self._get(uri)
        return response.json()

    def attach_metadata(self, timeseries_id, metadata_id_list):
        """
        Attach a list of metadata entries to a series.

        Parameters
        ----------
        timeseries_id : str
            id of timeseries
        metadata_id_list : list
            list of metadata_id

        Return
        ------
        dict
            response.json()
        """
        log.debug(f"attach_metadata with <token>, {timeseries_id}, {metadata_id_list}")

        uri = self._root + "{}/metadata".format(timeseries_id)

        response = self._put(uri, json=metadata_id_list)
        return response.json()

    def detach_metadata(self, timeseries_id, metadata_id_list):
        """
        Detach a list of metadata entries from a timeseries.

        Parameters
        ----------
        timeseries_id : str
            id of timeseries
        metadata_id_list : list
            list of metadata_id

        Return
        ------
        dict
            response.json()
        """
        log.debug(f"detach_metadata with <token>, {timeseries_id}, {metadata_id_list}")

        uri = self._root + "{}/metadata".format(timeseries_id)

        response = self._delete(uri, json=metadata_id_list)
        return response.json()
