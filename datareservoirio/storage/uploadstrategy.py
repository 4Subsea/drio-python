from __future__ import absolute_import, division, print_function

import logging

from ..log import LogWriter
from .storage_engine import AzureBlobService

logger = logging.getLogger(__name__)
log = LogWriter(logger)


class UploadStrategy(object):
    """Timeseries upload strategy that will upload Pandas dataframes to Azure Blob Storage."""

    def put(self, blob_params, series):
        return AzureBlobService(blob_params).create_blob_from_series(series)
