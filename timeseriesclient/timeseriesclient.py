import requests
import json
import numpy as np
import pandas as pd
import logging

from azure.storage.blob import BlockBlobService

from .adalwrapper import Authenticator, add_authorization_header
from . import globalsettings
from .fileupload import DataFrameUploader
from . import storage
from . import apitimeseries
from . import apifiles
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

class TimeSeriesClient(object):
    
    def __init__(self, host=None):
        self._authenticator = Authenticator()
        self._api_base_url = globalsettings.environment.api_base_url
        self._timeseries_api = apitimeseries.TimeSeriesApi()
        self._files_api = apifiles.FilesApi()

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
    
    def create(self, dataframe):
        self._verify_and_prepare_dataframe(dataframe)

        file_id = self._upload_file(dataframe)
        
        reference_time = self._get_reference_time(dataframe)
        response = self._timeseries_api.create(self.token, 
                                    file_id,  
                                    reference_time)
        
        return(response)

    def append(self, dataframe, timeseries_id):
        self._verify_and_prepare_dataframe(dataframe)
        
        file_id = self._upload_file(dataframe)
        
        response = self._timeseries_api.append(self.token, timeseries_id, file_id)

        return response

    def list_timeseries(self):
        return self._timeseries_api.list(self.token)

    def delete_timeseries(self, timeseries_id):
        return self._timeseries_api.delete(self.token, timeseries_id)

    def _upload_file(self, dataframe):
        upload_params =  self._files_api.upload(self.token)
        blobservice = storage.get_blobservice(upload_params)
        uploader = DataFrameUploader(blobservice)
        
        uploader.upload(dataframe, upload_params)

        self._files_api.commit(self.token, upload_params['FileId'])

        return upload_params['FileId']

    def _verify_and_prepare_dataframe(self, dataframe):
        logwriter.debug("checking arguments", "_check_arguments_create")

        if not isinstance(dataframe, pd.DataFrame):
            logwriter.error("dataframe type is {}".format(type(dataframe)))
            raise ValueError('dataframe must be a pandas DataFrame')

        if not dataframe.index.dtype in ['datetime64[ns]', 'int64']: 
            logwriter.error("index dtype is {}".format(dataframe.index.dtype)) 
            raise ValueError('allowed dtypes are datetime64[ns] and int64')

        if len(dataframe.keys()) > 1:
            logwriter.error("the dataframe has too many columns, currently only one column is supported")
            raise ValueError("the dataframe has too many columns, currently only one column is supported")

    def _get_reference_time(self, dataframe):
        if dataframe.index.dtype == 'datetime64[ns]':
            return str(np.datetime64(0, 's'))
        else:
            return None
        
        

        

    

        
