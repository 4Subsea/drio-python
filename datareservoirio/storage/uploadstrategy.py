from __future__ import absolute_import, division, print_function

import logging

from ..log import LogWriter
from .storage_engine import AzureBlobService

logger = logging.getLogger(__name__)
log = LogWriter(logger)


class UploadStrategy(object):
    """Timeseries upload strategy that will upload Pandas dataframes to Azure Blob Storage."""

    def __init__(self, session=None):
        """
        Initiate transfer service to Azure Blob Storage.

        :param: requests.Session session
            If specified, passed to the underlying BlockBlobService so that an existing
            request session can be reused.

        """
        self._session = session

    def put(self, blob_params, series):
        service = AzureBlobService(blob_params, session=self._session)
        return service.create_blob_from_series(series)
