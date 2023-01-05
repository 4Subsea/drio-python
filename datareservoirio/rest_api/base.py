import logging
from functools import wraps

from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry

from .. import globalsettings

log = logging.getLogger(__name__)


def _response_logger(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        log.debug("request initiated")
        response = func(*args, **kwargs)
        log.debug("response recieved")

        log.debug(f"request url: {response.request.url}")
        log.debug(f"status code: {response.status_code}")
        try:
            log.debug(f"response text: {response.text}")
        except ValueError:
            log.debug("response text: failed encoding")
        return response

    return func_wrapper


class BaseAPI:
    """Base class for reservoir REST API"""

    def __init__(self, session):
        self._api_base_url = globalsettings.environment.api_base_url

        self._session = session

        # Attention: Be careful when extending the list of retry_status!
        retry_status = frozenset([413, 429, 502, 503, 504])
        allowed_methods = frozenset(
            ["HEAD", "TRACE", "GET", "POST", "PUT", "OPTIONS", "DELETE"]
        )

        persist = Retry(
            total=10,
            backoff_factor=0.5,
            allowed_methods=allowed_methods,
            status_forcelist=retry_status,
            raise_on_status=False,
        )
        self._session.mount(self._api_base_url, HTTPAdapter(max_retries=persist))

        self._defaults = {"timeout": 30.0}

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


def _update_kwargs(kwargs, defaults):
    """Append defaults to keyword arguments"""
    for key in defaults:
        kwargs.setdefault(key, defaults[key])
