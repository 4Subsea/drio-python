import adal
import logging

from . import globalsettings
from . import usercredentials

class Authenticator(object):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._token = None

    def authenticate(self):
        params =  globalsettings.environment.get_adal_parameters()
        context = adal.AuthenticationContext(params.authority, api_version=None)

        username, password = usercredentials.get_user_credentials()

        self._token = context.acquire_token_with_username_password(params.resource,
                          username, 
                          password,
                          params.client_id)

    @property
    def token(self):
        return self._token

def add_authorization_header(header, token):
    key, value = create_authorization_header(token)
    header[key] = value
    return header

def create_authorization_header(token):
    key = 'Authorization'

    access_token = token['accessToken']
    value = 'Bearer {}'.format(access_token)

    return key, value
