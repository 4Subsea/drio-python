import adal
import logging
from datetime import datetime as dt
#import datetime
import numpy as np

from . import globalsettings
from . import usercredentials
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

class Authenticator(object):

    def __init__(self):
        self._token = None
        self._refresh_threshold = 10*60 # 10 minutes in seconds

        logwriter.debug("Instanciating authenticator")

    def authenticate(self):
        logwriter.debug("authenticating...", "authenticate")
        params = self._get_params()
        context = self._get_context()
        username, password = self._get_user_credentials()

        self._token = context.acquire_token_with_username_password(
                            params.resource,
                            username, 
                            password,
                            params.client_id)

    @property
    def token(self):
        if not self._token:
            logwriter.debug("token is None, calling authenticate", "token")
            self.authenticate()
        
        if self._time_until_token_expires() < self._refresh_threshold:
            logwriter.debug("time until token expires is less then threshold", "token")
            self.refresh_token()
 
        return self._token

    def refresh_token(self):
        logwriter.debug("refreshing token", "refresh_token")

        params = self._get_params()
        context = self._get_context()
        
        self._token = context.acquire_token_with_refresh_token(
                            self._token['refreshToken'], 
                            params.client_id, 
                            params.resource) 

    def _get_params(self):
        return globalsettings.environment.get_adal_parameters()

    def _get_context(self):
        params = self._get_params()
        context = adal.AuthenticationContext(params.authority, api_version=None)
        return context

    def _get_user_credentials(self):
        return usercredentials.get_user_credentials()

    def _time_until_token_expires(self):
        expires = np.datetime64(self._token.get('expiresOn'), 's')
        now = self._get_utcnow()
        
        return (expires - now).astype(int)

    def _get_utcnow(self):
        return np.datetime64(dt.utcnow().isoformat(), 's')
        

class UnsafeAuthenticator(Authenticator):

    def __init__(self, username, password):
        super(UnsafeAuthenticator, self).__init__()
        self.username = username
        self.password = password

    def _get_user_credentials(self):
        return self.username, self.password 


def add_authorization_header(header, token):
    key, value = create_authorization_header(token)
    header[key] = value
    return header

def create_authorization_header(token):
    key = 'Authorization'

    access_token = token['accessToken']
    value = 'Bearer {}'.format(access_token)

    return key, value
