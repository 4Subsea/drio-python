from __future__ import absolute_import

import logging
import getpass

import adal

from . import globalsettings
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class AdalAuthenticator(object):

    def __init__(self, username):
        logwriter.debug("Instanciating authenticator")

        self.params = self._get_params()
        self.context = adal.AuthenticationContext(self.params.authority,
                                                  api_version=None)

        self.username = username
        password = self._get_pass()

        self.context.acquire_token_with_username_password(self.params.resource,
                                                          self.username,
                                                          password,
                                                          self.params.client_id)
    @property
    def token(self):
        return self._token()

    def _token(self):
        self._token_cache = self.context.acquire_token(self.params.resource,
                                                       self.username,
                                                       self.params.client_id)
        logwriter.debug("token retrieved - expires {}"
                        .format(self._token_cache['expiresOn']), "_token")
        return self._token_cache

    def _get_params(self):
        return globalsettings.environment.get_adal_parameters()

    def _get_pass(self):
        return getpass.getpass('Password: ')


class UnsafeAdalAuthenticator(AdalAuthenticator):

    def __init__(self, username, password):
        self.password = password
        super(UnsafeAdalAuthenticator, self).__init__(username)

    def _get_pass(self):
        return self.password


Authenticator = AdalAuthenticator