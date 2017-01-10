from __future__ import absolute_import

import json
import logging
from functools import wraps

import requests

from .. import globalsettings
from ..log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


def _response_logger(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        logwriter.debug("response request url: {}".format(response.request.url))
        logwriter.debug("response status code: {}".format(response.status_code))
        try:
            logwriter.debug("response text: {}".format(response.text))
        except:
            logwriter.debug("response text: failed encoding")
        return response
    return func_wrapper


class BaseApi(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

    def _add_authorization_header(self, header, token):
        value = 'Bearer {}'.format(token['accessToken'])
        header['Authorization'] = value
        return header

    @_response_logger
    def _get(self, *args, **kwargs):
        response = requests.get(*args, **kwargs)
        return response

    @_response_logger
    def _post(self, *args, **kwargs):
        response = requests.post(*args, **kwargs)
        return response

    @_response_logger
    def _delete(self, *args, **kwargs):
        response = requests.delete( *args, **kwargs)
        return response
