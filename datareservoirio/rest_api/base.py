from __future__ import absolute_import, division, print_function

import logging
from functools import wraps

import requests
from requests.packages.urllib3 import Retry
from requests.adapters import HTTPAdapter

from .. import globalsettings
from ..log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


def _response_logger(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        logwriter.debug('request initiated')
        response = func(*args, **kwargs)
        logwriter.debug('response recieved')

        logwriter.debug('request url: {}'.format(response.request.url))
        logwriter.debug('status code: {}'.format(response.status_code))
        try:
            logwriter.debug('response text: {}'.format(response.text))
        except ValueError:
            logwriter.debug('response text: failed encoding')
        return response
    return func_wrapper


class BaseAPI(object):
    """Base class for reservoir REST API"""

    def __init__(self, session=None):
        self._api_base_url = globalsettings.environment.api_base_url

        self._session = requests.Session() if session is None else session

        # Attention: Be careful when extending the list of retry_status!
        retry_status = frozenset([413, 429, 500, 502, 503, 504])
        method_whitelist = frozenset(['HEAD', 'TRACE', 'GET', 'POST', 'PUT',
                                      'OPTIONS', 'DELETE'])

        persist = Retry(
            total=10, backoff_factor=0.5, method_whitelist=method_whitelist,
            status_forcelist=retry_status, raise_on_status=False)
        self._session.mount(self._api_base_url,
                            HTTPAdapter(max_retries=persist))

        self._defaults = {'timeout': 30.0}

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
    """Authenticator class for reservoir REST API"""
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        value = 'Bearer {}'.format(self.token['accessToken'])
        r.headers.update({'Authorization': value})
        return r


def _update_kwargs(kwargs, defaults):
    """Append defaults to keyword arguments"""
    for key in defaults:
        kwargs.setdefault(key, defaults[key])
