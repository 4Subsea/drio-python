import requests
import json
import numpy as np
import logging

from azure.storage.blob import BlockBlobService

from .adalwrapper import Authenticator, add_authorization_header
from . import globalsettings
from .fileupload import DataFrameUploader
from . import storage
from . import apitimeseries
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

class TimeSeriesClient(object):
    
    def __init__(self, host=None):
        self._authenticator = Authenticator()
        self._api_base_url = globalsettings.environment.api_base_url
        self._timeseries_api = apitimeseries.TimeSeriesApi()

    def authenticate(self):
        logwriter.debug("called", "authenticate")

        self._authenticator.authenticate()

    @property
    def token(self):
        logwriter.debug("called", "token")

        if not self._authenticator.token:
            logwriter.warning("returned token is None", "token")

        return self._authenticator.token

    def ping(self):
        uri = self._api_base_url + 'Ping'
        header = add_authorization_header({}, self.token)

        response = requests.get(uri, headers=header)
        return response
    
    def upload_timeseries(self, dataframe):
        upload_params =  self._get_file_upload_params()
        blobservice = storage.get_blobservice(upload_params)
        uploader = DataFrameUploader(blobservice)

        uploader.upload(dataframe, upload_params)

        self._commit_file(upload_params['FileId'])
        response = self._timeseries_api.create(self.token, 
                                    upload_params['FileId'],
                                    str(np.datetime64(0, 's')))
        
        del uploader, blobservice, upload_params

        return(response)

    def upload_file(self):
        return None

    def list_timeseries(self):
        return self._timeseries_api.list(self.token)

    def delete_timeseries(self, timeseries_id):
        return self._timeseries_api.delete(self.token, timeseries_id)

    # refactor into apiwrapper
    def _commit_file(self, file_id):
        uri = self._api_base_url + 'Files/commit'
        header = add_authorization_header({}, self.token)
        body = { 'FileId' : file_id }

        response = requests.post(uri, headers=header, data=body)

        return response.status_code

    # refactor into apiwrapper
    def _get_file_upload_params(self):
        uri = self._api_base_url + 'Files/upload'
        header = add_authorization_header({}, self.token)

        response = requests.post(uri, headers=header)
        upload_params = json.loads(response.text)

        upload_params['SasKey'] = upload_params['SasKey'].lstrip('?')    

        return upload_params


        
        

        

    

        
