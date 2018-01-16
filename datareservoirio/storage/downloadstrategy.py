from __future__ import absolute_import

import os
import logging
import sys
import base64
import shutil
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from ..log import LogWriter
from .storage_engine import AzureBlobService
from .simplefilecache import SimpleFileCache

logger = logging.getLogger(__name__)
log = LogWriter(logger)

class BaseDownloadStrategy:
    """
    Handle downloading of Data Reservoir chunks.
    
    Multiple chunks will be downloaded in parallel using ThreadPoolExecutor.
    """

    def get(self, response):
        filechunks = [f['Chunks'] for f in response['Files']]

        with ThreadPoolExecutor(max_workers=8) as executor:
            filedatas = executor.map(self._download_chunks_as_dataframe, filechunks)
            df = pd.DataFrame(columns=['index', 'values'])
            for fd in filedatas:
                if not fd.empty:
                    df = fd.combine_first(df)
            return df

    def _download_chunks_as_dataframe(self, chunks):
        if not chunks:
            return pd.DataFrame(columns=['index', 'values'])

        with ThreadPoolExecutor(max_workers=32) as executor:
            filechunks = executor.map(self._download_chunk, chunks)
            df_chunks = pd.concat(filechunks)
            df_chunks.sort_index(inplace=True)
            return df_chunks
    
    def _download_chunk(self, chunk):
        raise Exception('_download_chunk must be overridden in subclass')


class AlwaysDownloadStrategy(BaseDownloadStrategy):
    """Timeseries download strategy that will always download from the backend storage."""

    def _download_chunk(self, chunk):
        return AzureBlobService(chunk).get_blob_to_series()


class CachedDownloadStrategy(BaseDownloadStrategy):
    """
    Timeseries download strategy that will cache files in the current account's local APPDATA folder.
    
    Parameters
    ---------
    cache : cls
        Cache implementation, defaults to SimpleFileCache.

    """

    def __init__(self, cache=None):
        self._cache = cache if cache != None else SimpleFileCache()

    def _download_chunk(self, chunk):
        host = chunk['Account']
        container = chunk['Container']
        path = chunk['Path']
        md5 = base64.b64encode(chunk['ContentMd5'])

        return self._cache.get(
            lambda: self._blob_to_series(chunk),
            self._serialize_series,
            self._deserialize_series,
            host, container, path, md5)

    def _serialize_series(self, series, stream):
        series.to_csv(stream, header=False)

    def _deserialize_series(self, stream):
        return pd.read_csv(stream, header=None, names=['index', 'values'], index_col=0)

    @staticmethod
    def _blob_to_series(blob_params):
        return AzureBlobService(blob_params).get_blob_to_series()
