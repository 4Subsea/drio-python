from __future__ import absolute_import, division, print_function

from datetime import datetime, timedelta
import logging
from functools import update_wrapper, wraps
from threading import RLock as Lock

import requests
from requests.packages.urllib3 import Retry
from requests.adapters import HTTPAdapter

from .. import globalsettings
from ..log import LogWriter

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


def _response_logger(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        logwriter.debug("request initiated")
        response = func(*args, **kwargs)
        logwriter.debug("response recieved")

        logwriter.debug("request url: {}".format(response.request.url))
        logwriter.debug("status code: {}".format(response.status_code))
        try:
            logwriter.debug("response text: {}".format(response.text))
        except:
            logwriter.debug("response text: failed encoding")
        return response
    return func_wrapper


class BaseAPI(object):
    '''Base class for reservoir REST API'''

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

        self._session = requests.Session()
        retry_status = frozenset([413, 429, 500, 502, 503, 504])

        persist = Retry(total=10, backoff_factor=0.5,
                        status_forcelist=retry_status, raise_on_status=False)
        self._session.mount(self._api_base_url,
                            HTTPAdapter(max_retries=persist))

        self._defaults = {'timeout': 120.0}

    @_response_logger
    def _get(self, *args, **kwargs):
        _update_kwargs(kwargs, self._defaults)
        response = self._session.get(*args, **kwargs)
        response.raise_for_status()
        return response

    @_response_logger
    def _post(self, *args, **kwargs):
        _update_kwargs(kwargs, self._defaults)
        response = self._session.post(*args, **kwargs)
        response.raise_for_status()
        return response

    @_response_logger
    def _put(self, *args, **kwargs):
        _update_kwargs(kwargs, self._defaults)
        response = self._session.put(*args, **kwargs)
        response.raise_for_status()
        return response

    @_response_logger
    def _delete(self, *args, **kwargs):
        _update_kwargs(kwargs, self._defaults)
        response = self._session.delete(*args, **kwargs)
        response.raise_for_status()
        return response


class TokenAuth(requests.auth.AuthBase):
    '''Authenticator class for reservoir REST API'''
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        value = 'Bearer {}'.format(self.token['accessToken'])
        r.headers.update({'Authorization': value})
        return r


def _update_kwargs(kwargs, defaults):
    '''Append defaults to keyword arguments'''
    for key in defaults:
        kwargs.setdefault(key, defaults[key])
