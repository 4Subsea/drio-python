import requests

from .adalwrapper import Authenticator
from . import globalsettings

class TimeSeriesClient(object):
    
    def __init__(self, host=None):
        self._authenticator = Authenticator()
        self._api_base_url = globalsettings.environment.api_base_url

    def authenticate(self):
        self._authenticator.authenticate()

    @property
    def token(self):
        return self._authenticator.token

    def ping(self):
        uri = self._api_base_url + 'Ping'

        header = self._add_authorization_header({})

        response = requests.get(uri, headers=header)
        return response

    def _add_authorization_header(self, header):
        key, value = self._create_authorization_header()
        header[key] = value
        return header

    def _create_authorization_header(self):
        key = 'Authorization'

        access_token = self.token['accessToken']
        value = 'Bearer {}'.format(access_token)

        return key, value
        
        

        

    

        
