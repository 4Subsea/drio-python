import requests
import json
from azure.storage.blob import BlockBlobService

from .adalwrapper import Authenticator
from . import globalsettings
from .fileupload import DataFrameUploader
from . import storage

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
    
    def upload_timeseries(self, dataframe):
        upload_params =  self._get_file_upload_params()
        blobservice = storage.get_blobservice(upload_params)
        uploader = DataFrameUploader(blobservice)

        uploader.upload(dataframe, upload_params)
        
        del uploader, blobservice, upload_params

    def upload_file(self):
        return None

    # refactor into apiwrapper
    def _get_file_upload_params(self):
        uri = self._api_base_url + 'Files/upload'
        header = self._add_authorization_header({})

        response = requests.post(uri, headers=header)
        upload_params = json.loads(response.text)

        upload_params['SasKey'] = upload_params['SasKey'].lstrip('?')    

        return upload_params

    def _add_authorization_header(self, header):
        key, value = self._create_authorization_header()
        header[key] = value
        return header

    # refactor -> put into adalwrapper
    def _create_authorization_header(self):
        key = 'Authorization'

        access_token = self.token['accessToken']
        value = 'Bearer {}'.format(access_token)

        return key, value

        
        

        

    

        
