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

class BaseDownloadStrategy(object):
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
    Data Reservoir timeseries chunk download strategy with local disk caching.
    
    Files will be cached in the current account's local APPDATA folder.
    Chunks in the cache will be organized according to file format and blob location
    within the remote Azure Storage.

    """

    class MsgPackFormat(object):
        """Serialize dataframe to/from the msgpack format."""

        @property
        def file_extension(self):
            return 'mp'

        def serialize(self, dataframe, stream):
            dataframe.to_msgpack(stream)

        def deserialize(self, stream):
            return pd.read_msgpack(stream)

    class CsvFormat(object):
        """Serialize dataframe to/from the csv format."""

        @property
        def file_extension(self):
            return 'csv'

        def serialize(self, dataframe, stream):
            dataframe.to_csv(stream, header=True)

        def deserialize(self, stream):
            return pd.read_csv(stream, index_col=0)

    def __init__(self, cache=None, format='msgpack'):
        """
        Initialize a dataframe download strategy using a cache implementation and a serialization format.

        Parameters
        ---------
        :param: cls cache
            Cache implementation, defaults to SimpleFileCache.
        :param: str format
            Serialization format of the files stored in cache. Supports 'msgpack' and 'csv'. Default is 'msgpack'.

        """
        if format == 'msgpack':
            self._format = self.MsgPackFormat()
        elif format == 'csv':
            self._format = self.CsvFormat()
        else:
            raise ValueError('Unsupported format ' + format)

        self._cache = cache if cache is not None else SimpleFileCache()

    def _download_chunk(self, chunk):
        key0 = self._format.file_extension
        key1 = chunk['Account']
        key2 = chunk['Container']
        path = chunk['Path']
        file = base64.b64encode(chunk['ContentMd5'])

        return self._cache.get(
            lambda: self._blob_to_series(chunk),
            self._serialize_series,
            self._deserialize_series,
            key0, key1, key2, path, file)

    def _serialize_series(self, series, stream):
        self._format.serialize(series, stream)

    def _deserialize_series(self, stream):
        return self._format.deserialize(stream)

    @staticmethod
    def _blob_to_series(blob_params):
        return AzureBlobService(blob_params).get_blob_to_series()
