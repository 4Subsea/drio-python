from __future__ import absolute_import, division, print_function

import base64
import logging
import codecs
from concurrent.futures import ThreadPoolExecutor
from functools import partial

try:
    from itertools import imap as map
except ImportError:
    pass

import pandas as pd

from ..log import LogWriter
from .storage_engine import AzureBlobService

logger = logging.getLogger(__name__)
log = LogWriter(logger)


class BaseDownloadStrategy(object):
    """
    Handle downloading of Data Reservoir chunks.

    Multiple chunks will be downloaded in parallel using ThreadPoolExecutor.
    """

    def get(self, response):
        filechunks = [f['Chunks'] for f in response['Files']]
        filedatas = map(self._download_chunks_as_dataframe, filechunks)

        try:
            series = next(filedatas)
        except StopIteration:
            return pd.Series()

        for fd in filedatas:
                series = self._combine_first(fd, series)
        return series

    def _download_chunks_as_dataframe(self, chunks):
        if not chunks:
            return pd.Series()

        with ThreadPoolExecutor() as executor:
            filechunks = executor.map(self._download_verified_chunk, chunks)
        series_chunks = pd.concat(filechunks)
        return series_chunks

    def _download_verified_chunk(self, chunk):
        """
        Download chunk as pandas Series and ensure the series does not contain
        duplicates.
        """
        df = self._download_chunk(chunk)
        if not df.index.is_unique:
            return df[~df.index.duplicated(keep='last')]
        return df

    def _download_chunk(self, chunk):
        raise Exception('_download_chunk must be overridden in subclass')

    @staticmethod
    def _combine_first(calling, other):
        """
        Faster combine first for most common scenarios and fall back to general
        purpose pandas combine_first for advanced cases.
        """
        if calling.empty or other.empty:
            return calling

        calling_index = calling.index.values
        other_index = other.index.values

        late_start = max([calling_index[0], other_index[0]])
        early_end = min([calling_index[-1], other_index[-1]])

        if late_start > early_end:  # no overlap
            if calling_index[0] < late_start:
                df_combined = pd.concat((calling, other))
            else:
                df_combined = pd.concat((other, calling))
        elif (len(calling_index) == len(other_index) and
              (calling_index == other_index).all()):  # exact overlap
            df_combined = calling
        else:  # partial overlap - expensive
            df_combined = calling.combine_first(other)

        return df_combined

    @staticmethod
    def _get_chunks_hash(response):
        chunks_all = [chunk['ContentMd5'] for f in response['Files']
                      for chunk in f['Chunks']]
        return hash(''.join(chunks_all))


class AlwaysDownloadStrategy(BaseDownloadStrategy):
    """Timeseries download strategy that will always download from the backend storage."""

    def __init__(self, session=None):
        self._session = session

    def _download_chunk(self, chunk):
        service = AzureBlobService(chunk, session=self._session)
        return service.get_blob_to_series()


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

        def __init__(self):
            self._reader_factory = codecs.getreader('utf-8')
            self._writer_factory = codecs.getwriter('utf-8')

        @property
        def file_extension(self):
            return 'csv'

        def serialize(self, dataframe, stream):
            with self._writer_factory(stream) as sw:
                dataframe.to_csv(sw, header=True, encoding='ascii')

        def deserialize(self, stream):
            with self._reader_factory(stream) as sr:
                return pd.read_csv(sr, index_col=0, encoding='ascii')

    def __init__(self, cache, format='msgpack', session=None):
        """
        Initialize a dataframe download strategy using a cache implementation
        and a serialization format.

        Parameters
        ---------
        :param: cls cache
            Cache implementation, see SimpleFileCache.
        :param: str format
            Serialization format of the files stored in cache. Supports
            'msgpack' and 'csv'. Default is 'msgpack'.
        :param: requests.Session session
            Request session to reuse when making requests to Storage
        """
        super(CachedDownloadStrategy, self).__init__()

        if format == 'msgpack':
            self._format = self.MsgPackFormat()
        elif format == 'csv':
            self._format = self.CsvFormat()
        else:
            raise ValueError('Unsupported format {}'.format(format))

        self._cache = cache
        self._memory_cache = (hash(None), None)
        self._session = session

    def get(self, response):
        # short-circuit if same as last one
        chunks_hash = self._get_chunks_hash(response)
        chunks_hash_cached, series_cached = self._memory_cache
        if chunks_hash == chunks_hash_cached:
            return series_cached

        series = super(CachedDownloadStrategy, self).get(response)
        self._memory_cache = (chunks_hash, series)
        return series

    def _download_chunk(self, chunk):
        key0 = self._format.file_extension
        key1 = chunk['Account']
        key2 = chunk['Container']
        path = chunk['Path']
        file_ = self._encode_for_path_safety(chunk['ContentMd5'])

        return self._cache.get(
            partial(self._blob_to_series, chunk),
            self._serialize_series,
            self._deserialize_series,
            key0, key1, key2, path, file_)

    def _serialize_series(self, series, stream):
        self._format.serialize(series, stream)

    def _deserialize_series(self, stream):
        return self._format.deserialize(stream)

    def _blob_to_series(self, blob_params):
        service = AzureBlobService(blob_params, session=self._session)
        return service.get_blob_to_series()

    @staticmethod
    def _encode_for_path_safety(value):
        return str(base64.urlsafe_b64encode(str(value).encode()).decode())
