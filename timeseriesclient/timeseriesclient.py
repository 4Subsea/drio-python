import json
import logging
import time
import timeit
import cStringIO

import requests
import numpy as np
import pandas as pd
from azure.storage.blob import BlockBlobService

#from .adalwrapper import Authenticator, add_authorization_header
from . import globalsettings
from .fileupload import DataFrameUploader
from . import storage
from . import apitimeseries
from . import apifiles
from .log import LogWriter


logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

# Default values to push as start/end dates. (Limited by numpy.datetime64)
_END_DEFAULT = 9214646400000000000  # 2262-01-01
_START_DEFAULT = -9214560000000000000  # 1678-01-01


class TimeSeriesClient(object):
    """
    The TimeSeriesClient handles requests, data uploads, and data downloads
    from the 4Subsea data reservoir.

    Parameters
    ---------
    auth : cls
        An authenticator class with :attr:`token` attribute that provides a valid
        token to the 4Subsea data reservoir.

    Warning
    -------
    A time series is identified by its unique identifier, its id. If this id is
    lost, the time series is essentially lost.
    """

    def __init__(self, auth):
        self._authenticator = auth
        #self._api_base_url = globalsettings.environment.api_base_url
        self._timeseries_api = apitimeseries.TimeSeriesApi()
        self._files_api = apifiles.FilesApi()

    @property
    def token(self):
        """
        Your token that is sent to the data reservoir with every request you 
        make. There is no password stored in the token, it only provides access 
        for a limited amount of time and only to the data reservoir.
        """
        logwriter.debug("called", "token")

        if not self._authenticator.token:
            logwriter.error("returned token is None", "token")

        return self._authenticator.token

    def ping(self):
        """
        With ping you can test that you have a working connection to the data
        reservoir.
        """
        return self._files_api.ping(self.token)

    def create(self, dataframe):
        """
        Create a new time series in the data reservoir from a dataframe.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            the dataframe must contain exactly one column (plus the index). The
            index must either be a numpy.datetime64 (with nanosecond precision) 
            or numpy.int64.

        Returns
        -------
        dict 
            The response from the data reservoir
        """
        self._verify_and_prepare_dataframe(dataframe)

        start = timeit.default_timer()
        file_id = self._upload_file(dataframe)
        current = timeit.default_timer()
        logwriter.info('Fileupload took {} seconds'.format(current - start), 'create')

        status = self._wait_until_file_ready(file_id)
        if status == "Failed":
            return status
        current = timeit.default_timer()
        logwriter.info('Processing serverside time elapsed since start is {} seconds'.format(current - start), 'create')

        response = self._timeseries_api.create(self.token,
                                    file_id)
        current = timeit.default_timer()
        logwriter.info('Done, total time spent: {} seconds ({} minutes)'.format(current - start, (current - start)/60.), 'create')

        
        return(response)

    def append(self, dataframe, timeseries_id):
        """
        Append data to an already existing time series.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            the dataframe must contain exactly one column (plus the index). The
            index must either be a numpy.datetime64 (with nanosecond precision) 
            or integer array. 
        timeseries_id : string
            the identifier of the timeseries must exist.

        Returns
        -------
        dict 
            The response from the data reservoir
        """
        self._verify_and_prepare_dataframe(dataframe)

        file_id = self._upload_file(dataframe)
        status = self._wait_until_file_ready(file_id)
        if status == "Failed":
            return status
        response = self._timeseries_api.add(self.token, timeseries_id,
                                            file_id)
        return response

    def list(self):
        """
        Lists all available timeseries in the reservoir. 

        Returns
        -------
        list 
            All timeseries ids in the reservoir.
        """
        return self._timeseries_api.list(self.token)

    def info(self, timeseries_id):
        """
        Retrieves basic information about one timeseries.
        
        Returns
        -------
        dict 
            Available information about the timeseries. None if timeseries not 
            found.
        """
        return self._timeseries_api.info(self.token, timeseries_id)

    def delete(self, timeseries_id):
        """
        Parameters
        ----------
        timeseries_id : string
            The id of the timeseries to delete.

        Returns
        -------
        int 
            http status code. 200 is OK
        """
        return self._timeseries_api.delete(self.token, timeseries_id)

    def get(self, timeseries_id, start=None, end=None, convert_date=False):
        """
        Retrieves a timeseries from the data reservoir.

        Parameters
        ----------
        timeseries_id : str
            id of the timeseries to download
        start : optional
            start time (inclusive) of the timeseries given as anything
            pandas.to_datetime is able to parse.
        end : optional
            stop time (inclusive) of the timeseries given as anything
            pandas.to_datetime is able to parse.
        convert_date : bool
            If True, the index is converted to numpy.datetime64[ns]. Default is
            False, index is returned as nanoseconds since epoch.

        Returns
        -------
        pandas.Series
            Timeseries data
        """
        if not start:
            start = _START_DEFAULT
        if not end:
            end = _END_DEFAULT

        start = pd.to_datetime(start, dayfirst=True, unit='ns').value
        end = pd.to_datetime(end, dayfirst=True, unit='ns').value

        if start >= end:
            raise ValueError('start must be before end')

        response = self._timeseries_api.data(self.token, timeseries_id, start,
                                             end)

        response_txt = cStringIO.StringIO(response.text)
        df = pd.read_csv(response_txt, header=None,
                         names=['time', 'values'], index_col=0)
        response_txt.close()

        if convert_date:
            df.index = pd.to_datetime(df.index)
        return df['values']

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

    def _wait_until_file_ready(self, file_id):
        #wait for server side processing
        while True:
            status = self._get_file_status(file_id)
            logwriter.debug("status is {}".format(status), "create")

            if status == "Ready":
                return "Ready"
            elif status == "Failed":
                return "Failed"

            time.sleep(5)

    def _get_file_status(self, file_id):
        response = self._files_api.status(self.token, file_id)
        return response['State']
