from __future__ import absolute_import

import json
import logging
import sys
import time
import timeit

import numpy as np
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

from .log import LogWriter
from .rest_api import FilesAPI, TimeSeriesAPI

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

# Default values to push as start/end dates. (Limited by numpy.datetime64)
_END_DEFAULT = 9214646400000000000  # 2262-01-01
_START_DEFAULT = -9214560000000000000  # 1678-01-01


class Client(object):
    """
    Client class handles requests, data uploads, and data downloads
    from the 4Subsea data reservoir.

    Parameters
    ---------
    auth : cls
        An authenticator class with :attr:`token` attribute that provides a valid
        token to the 4Subsea data reservoir.
    """

    def __init__(self, auth):
        self._authenticator = auth
        # self._api_base_url = globalsettings.environment.api_base_url
        self._timeseries_api = TimeSeriesAPI()
        self._files_api = FilesAPI()

    @property
    def token(self):
        """
        Your token that is sent to the data reservoir with every request you
        make. There is no password stored in the token, it only provides access 
        for a limited amount of time and only to the data reservoir.
        """
        return self._authenticator.token

    def ping(self):
        """
        With ping you can test that you have a working connection to the data
        reservoir.
        """
        return self._files_api.ping(self.token)

    def create(self, series):
        """
        Create a new series in the reservoir from a pandas.Series.

        Parameters
        ----------
        series : pandas.Series
            Series with index (as numpy.datetime64 (with nanosecond precision)
            or integer array).

        Returns
        -------
        dict
            The response from the reservoir
        """
        self._verify_and_prepare_series(series)

        time_start = timeit.default_timer()
        file_id = self._upload_series(series)
        time_upload = timeit.default_timer()
        logwriter.info('Fileupload took {} seconds'
                       .format(time_upload - time_start), 'create')

        status = self._wait_until_file_ready(file_id)
        time_process = timeit.default_timer()
        logwriter.info('Processing serverside took {} seconds'
                       .format(time_process - time_upload), 'create')
        if status == "Failed":
            return status

        response = self._timeseries_api.create(self.token, file_id)
        time_end = timeit.default_timer()
        logwriter.info('Done, total time spent: {} seconds ({} minutes)'
                       .format(time_end - time_start, (time_end - time_start) / 60.), 'create')
        return response

    def append(self, series, series_id):
        """
        Append data to an already existing series.

        Parameters
        ----------
        series : pandas.Series
            Series with index (as numpy.datetime64 (with nanosecond precision)
            or integer array).
        series_id : string
            the identifier of the existing series.

        Returns
        -------
        dict
            The response from the reservoir
        """
        self._verify_and_prepare_series(series)

        time_start = timeit.default_timer()
        file_id = self._upload_series(series)
        time_upload = timeit.default_timer()
        logwriter.info('Upload took {} seconds'
                       .format(time_upload - time_start), 'append')

        status = self._wait_until_file_ready(file_id)
        time_process = timeit.default_timer()
        logwriter.info('Processing serverside took {} seconds'
                       .format(time_process - time_upload), 'append')
        if status == "Failed":
            return status

        time_end = timeit.default_timer()
        logwriter.info('Done, total time spent: {} seconds ({} minutes)'
                       .format(time_end - time_start, (time_end - time_start) / 60.), 'append')

        response = self._timeseries_api.add(self.token, series_id, file_id)
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

    def get(self, timeseries_id, start=None, end=None, convert_date=False,
            raise_empty=False):
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
        raise_empty : bool
            If True, raise ValueError if no data exist in the provided interval. Otherwise,
            return an empty pandas.Series (default).

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

        time_start = timeit.default_timer()

        logwriter.debug("getting chunks list")
        response = self._timeseries_api.download_days(self.token, timeseries_id, start, end)

        df = self._download_series(response)

        if not df.empty:
            df = df.loc[start:end]

        if df.empty and raise_empty:  # may become empty after slicing
            raise ValueError('can\'t find data in the given interval')

        if convert_date:
            df.index = pd.to_datetime(df.index)

        time_end = timeit.default_timer()
        logwriter.info('Download timeseries to dataframe took {} seconds'
                       .format(time_end - time_start), 'get')

        return df['values']

    def _download_series(self, params):
        filechunks = [f['Chunks'] for f in params['Files']]
        # download chunks, one by one
        filedatas = [
            self._download_chunks_as_dataframe(chunk) for chunk in filechunks]
        df = pd.DataFrame()
        for fd in filedatas:
            df = fd.combine_first(df)
        return df

    def _download_chunks_as_dataframe(self, chunks):
        if not chunks:
            return pd.DataFrame(columns=['index', 'values'])
        with ThreadPoolExecutor(max_workers=32) as executor:
            filechunks = executor.map(
                lambda chunk: self._files_api.transfer_service(
                    chunk).get_blob_to_series(),
                chunks)
            
            return pd.concat(filechunks)

    def _upload_series(self, series):
        upload_params = self._files_api.upload(self.token)
        uploader = self._files_api.transfer_service(upload_params)
        uploader.create_blob_from_series(series)
        self._files_api.commit(self.token, upload_params['FileId'])
        return upload_params['FileId']

    def _verify_and_prepare_series(self, series):
        logwriter.debug("checking arguments", "_check_arguments_create")

        if not isinstance(series, pd.Series):
            logwriter.error("series type is {}".format(type(series)))
            raise ValueError('series must be a pandas Series')

        if not (pd.api.types.is_datetime64_ns_dtype(series.index) or
                pd.api.types.is_int64_dtype(series.index)):
            logwriter.error("index dtype is {}".format(series.index.dtype))
            raise ValueError('allowed dtypes are datetime64[ns] and int64')

    def _wait_until_file_ready(self, file_id):
        # wait for server side processing
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
