from __future__ import absolute_import

import logging
import sys

from ..log import LogWriter
from .downloadstrategy import CachedDownloadStrategy
from .uploadstrategy import UploadStrategy

if sys.version_info.major == 3:
    from urllib.parse import urlparse
elif sys.version_info.major == 2:
    from urlparse import urlparse


logger = logging.getLogger(__name__)
log = LogWriter(logger)


class Storage(object):
    """
    Handle download and upload of timeseries data in the Data Reservoir
    storage.
    """

    def __init__(self, authenticator, timeseries_api, files_api,
                 downloader=CachedDownloadStrategy(),
                 uploader=UploadStrategy()):
        """
        Initialize service for working with timeseries data in Azure Blob
        Storage.

        Parameters
        ----------
        authenticator: object
            Token provider (must support auth.token()).
        timeseries_api: TimeseriesAPI
            Instance of timeseries API.
        files_api: FilesAPI
            Instance of files API.
        downloader: cls
            A strategy instance for handling downloads.
        uploader: cls
            A strategy instance for handling uploads.

        """
        self._authenticator = authenticator
        self._timeseries_api = timeseries_api
        self._files_api = files_api
        self._downloader = downloader
        self._uploader = uploader

    @property
    def token(self):
        return self._authenticator.token

    def put(self, series):
        """
        Put a data range into storage.

        Parameters
        ----------
        series : pandas.Series
            pandas Series to store

        Returns
        -------
            The unique file id as stored in the reservoir

        """
        upload_params = self._files_api.upload(self.token)
        file_id = upload_params['FileId']

        self._uploader.put(upload_params, series)

        self._files_api.commit(self.token, file_id)
        return file_id

    def get(self, timeseries_id, start, end):
        """
        Get a range of data for a timeseries.

        Parameters
        ----------
        timeseries_id: guid
            Unique id of the timeseries
        start: long
            Start time of the range, in nanoseconds since EPOCH
        end: long
            End time of the range, in nanoseconds since EPOCH

        """
        log.debug("getting day file inventory")
        response = self._timeseries_api.download_days(
            self.token, timeseries_id, start, end)

        df = self._downloader.get(response)

        if not df.empty:
            df = df.loc[start:end]

        return df
