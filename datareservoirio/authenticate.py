from __future__ import absolute_import, division, print_function

import getpass
import logging
import threading

import adal

from . import _constants
from .globalsettings import environment
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class ADALParameters(object):

    def __init__(self, environment):
        if environment == _constants.ENV_TEST:
            self.set_test()
        elif environment == _constants.ENV_QA:
            self.set_qa()
        elif environment == _constants.ENV_PROD:
            self.set_prod()
        elif environment == _constants.ENV_DEV:
            self.set_dev()

    @property
    def resource(self):
        return self._resource

    @property
    def client_id(self):
        return self._client_id

    @property
    def authority(self):
        return self._authority

    def set_test(self):
        self._resource = _constants.RESOURCE_TEST
        self.set_shared()

    def set_qa(self):
        self._resource = _constants.RESOURCE_QA
        self.set_shared()

    def set_prod(self):
        self._resource = _constants.RESOURCE_PROD
        self.set_shared()

    def set_dev(self):
        self._resource = _constants.RESOURCE_DEV
        self.set_shared()

    def set_shared(self):
        self._client_id = _constants.CLIENT_ID
        self._authority = _constants.AUTHORITY


class AdalAuthenticator(object):

    def __init__(self, username):
        logwriter.debug("Instanciating authenticator")

        self.params = self._get_params()
        self.context = adal.AuthenticationContext(
            self.params.authority, api_version=None)

        self.username = username
        password = self._get_pass()

        self._token_lock = threading.Lock()

        self.context.acquire_token_with_username_password(
            self.params.resource, self.username, password,
            self.params.client_id)

    @property
    def token(self):
        return self._token()

    def _token(self):
        with self._token_lock:
            self._token_cache = self.context.acquire_token(
                self.params.resource, self.username, self.params.client_id)

        logwriter.debug("token retrieved - expires {}"
                        .format(self._token_cache['expiresOn']), "_token")
        return self._token_cache

    def _get_params(self):
        return ADALParameters(environment.current_environment)

    def _get_pass(self):
        return getpass.getpass('Password: ')


class UnsafeAdalAuthenticator(AdalAuthenticator):

    def __init__(self, username, password):
        self.password = password
        super(UnsafeAdalAuthenticator, self).__init__(username)

    def _get_pass(self):
        return self.password


Authenticator = AdalAuthenticator
