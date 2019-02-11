import logging
import sys
from urllib.parse import urlparse

import pandas as pd

from ..log import LogWriter
from .downloadstrategy import CachedDownloadStrategy
from .uploadstrategy import UploadStrategy


logger = logging.getLogger(__name__)
log = LogWriter(logger)


class Storage(object):
    """
    Handle download and upload of timeseries data in the Data Reservoir
    storage.
    """

    def __init__(self, timeseries_api, files_api, downloader,
                 uploader):
        """
        Initialize service for working with timeseries data in Azure Blob
        Storage.

        Parameters
        ----------
        timeseries_api: TimeseriesAPI
            Instance of timeseries API.
        files_api: FilesAPI
            Instance of files API.
        downloader: cls
            A strategy instance for handling downloads.
        uploader: cls
            A strategy instance for handling uploads.

        """
        self._timeseries_api = timeseries_api
        self._files_api = files_api
        self._downloader = downloader
        self._uploader = uploader

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
        upload_params = self._files_api.upload()
        file_id = upload_params['FileId']

        self._uploader.put(upload_params, series)

        self._files_api.commit(file_id)
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
        response = self._timeseries_api.download_days(timeseries_id, start, end)

        series = self._downloader.get(response)

        # at this point, an entirely new object w/o reference
        # to internal objects is created.
        if series.empty:
            return pd.Series()
        return self._create_series(series, start, end)

    def _create_series(self, series, start, end):
        """Create a new pandas Series w/o internal references"""
        index_bool = (series.index.values >= start) &\
                     (series.index.values <= end)
        index = series.index.values[index_bool]  # enforces copy
        values = series.values[index_bool]  # enforces copy
        return pd.Series(data=values, index=index)
