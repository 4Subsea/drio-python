from __future__ import absolute_import, division, print_function

from datetime import datetime, timedelta
import logging
from functools import update_wrapper
from threading import RLock as Lock

from ..log import LogWriter
from .base import BaseAPI, TokenAuth

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


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


def _request_cache_wrapper(func, timeout, maxsize, skip_argpos=2):
    sentinel = object()  # unique object used to signal cache misses
    cache = {}

    def wrapper(*args, **kwargs):
        timestamp = datetime.utcnow()

        request_signature = _make_request_hash(args[skip_argpos:], kwargs)

        logwriter.debug('attempting to access request cache')
        result, timestamp_cache = cache.get(request_signature,
                                            (sentinel, None))

        if (result is sentinel or
                (timestamp_cache + timedelta(seconds=timeout) < timestamp)):
            logwriter.debug('request cache invalid - acquiring from source')
            result = func(*args, **kwargs)

            with Lock():  # altering cache is not thread-safe
                cache[request_signature] = (result, timestamp)
                if len(cache) > maxsize:
                    logwriter.debug('request cache full - pop out oldest item')
                    key = sorted(cache, key=lambda x: cache.get(x)[-1])[0]
                    del cache[key]
        return result

    wrapper._cache = cache
    return wrapper


def _make_request_hash(args, kwargs):
    """Hash request signature."""
    key = args
    for kwarg in kwargs.iteritems():
        key += kwarg
    return hash(key)


class TimeSeriesAPI(BaseAPI):
    """Python wrapper for reservoir-api.4subsea.net/api/timeseries."""

    def __init__(self, cache=True):
        super(TimeSeriesAPI, self).__init__()
        self._cache = cache

    def create(self, token, file_id):
        """
        Create timeseries entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        file_id : str
            id of file (Files API) to be tied to timeseries entry.

        Return
        ------
        dict
            http response.text loaded as json
        """
        logwriter.debug("called with <token>, {}".format(file_id), "create")

        uri = self._api_base_url + 'timeseries/create'
        body = {"FileId": file_id}
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.json()

    def add(self, token, timeseries_id, file_id):
        """
        Append timeseries data to an existing entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries
        file_id : str
            id of file (File API) to be appended.

        Notes
        -----
        Refer to API documentation wrt apppend, overlap, and overwrite behavior
        """
        logwriter.debug("called with <token>, {}, {}".format(
            timeseries_id, file_id), "add")

        uri = self._api_base_url + 'timeseries/add'
        body = {"TimeSeriesId": timeseries_id, "FileId": file_id}
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.json()

    def info(self, token, timeseries_id):
        """
        Information about a timeseries entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries

        Return
        ------
        dict
            dictionary containing information about a timeseries
            entry in the reservoir
        """
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def delete(self, token, timeseries_id):
        """
        Delete a timeseries.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries
        """
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        response = self._delete(uri, auth=TokenAuth(token))
        return

    def download_days(self, token, timeseries_id, start, end):
        """
        Return timeseries data with start/stop.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
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
        logwriter.debug("called with <token>, {}, {}, {}".format(
            timeseries_id, start, end))

        nanoseconds_day = 86400000000000
        start = (start//nanoseconds_day)*nanoseconds_day
        end = ((end//nanoseconds_day) + 1)*nanoseconds_day - 1

        download_days = (self._download_days_cached
                         if self._cache else self._download_days_base)
        return download_days(token, timeseries_id, start, end)

    @request_cache(timeout=180)
    def _download_days_cached(self, *args):
        return self._download_days_base(*args)

    def _download_days_base(self, token, timeseries_id, start, end):
        """
        Return timeseries data with start/stop.
        Internal low level function to allow for higher level operations on
        public counterpart.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
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
        logwriter.debug("called with <token>, {}, {}, {}".format(
            timeseries_id, start, end))

        uri = self._api_base_url + 'timeseries/{}/download/days'.format(timeseries_id)
        params = {'start': start, 'end': end}

        response = self._get(uri, params=params, auth=TokenAuth(token))
        return response.json()

    def attach_metadata(self, token, timeseries_id, metadata_id_list):
        """
        Attach a list of metadata entries to a series.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of timeseries
        metadata_id_list : list
            list of metadata_id

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            timeseries_id, metadata_id_list), "attach_metadata")

        uri = self._api_base_url + \
            'timeseries/{}/attachMetadata'.format(timeseries_id)

        response = self._post(uri, json=metadata_id_list,
                              auth=TokenAuth(token))
        return response.json()

    def detach_metadata(self, token, timeseries_id, metadata_id_list):
        """
        Detach a list of metadata entries from a timeseries.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of timeseries
        metadata_id_list : list
            list of metadata_id

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            timeseries_id, metadata_id_list), "attach_metadata")

        uri = self._api_base_url + \
            'timeseries/{}/detachMetadata'.format(timeseries_id)

        response = self._delete(uri, json=metadata_id_list,
                                auth=TokenAuth(token))
        return response.json()
