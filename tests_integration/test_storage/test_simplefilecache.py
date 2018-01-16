import unittest
import logging
import sys
import os
import pandas as pd
from timeit import default_timer as timer
from mock import patch

import datareservoirio
from datareservoirio.storage import SimpleFileCache

log = logging.getLogger(__file__)


class Test_SimpleFileCache(unittest.TestCase):
    def setUp(self):

        rows = [(0, 42.0), (1, 37.0), (2, 1.54)]
        self._data = pd.DataFrame.from_records(rows, columns=['index', 'values'])

        self.cache = SimpleFileCache(cache_folder='reservoir_cache_integration', compressionOn=False)
        self.cache.reset_cache()
    
    def test_reset_cache_deletes_any_file_in_cache(self):
        rows = [(0, 1.0), (1, 2.0), (2, 3.0)]
        newdata = pd.DataFrame.from_records(rows, columns=['index', 'values'])
        key = 'test_reset_cache_deletes_any_file_in_cache\\data'

        cacheddata = self.cache.get(
            lambda: self._data,
            self._data_to_csv,
            self._csv_to_data,
            key)
        
        self.cache.reset_cache()

        newcacheddata = self.cache.get(
            lambda: newdata,
            self._data_to_csv,
            self._csv_to_data,
            key)

        self.assertTrue(newcacheddata.equals(newdata))
        self.assertFalse(newcacheddata.equals(self._data))

    def test_get_when_serializer_throws_does_not_create_cache_file(self):
        rows = [(0, 1.0), (1, 2.0), (2, 3.0)]
        newdata = pd.DataFrame.from_records(rows, columns=['index', 'values'])
        key = 'test_get_when_serializer_throws_does_not_create_cache_file\\data'

        try:
            self.cache.get(
                lambda: 'nada',
                lambda data, stream: 1/0,   # raise unhandled exception
                lambda stream: 'nix',
                key)
        except Exception:
            pass

        filepath = self.cache._get_cache_filepath_for(key)
        self.assertFalse(os.path.exists(filepath))

    def test_get_with_empty_cache(self):
        key = 'test_get_with_empty_cache\\data'

        cacheddata = self.cache.get(
            lambda: self._data,
            self._data_to_csv,
            self._csv_to_data,
            key)

        self.assertTrue(cacheddata.equals(self._data))

    def test_get_without_compression(self):
        key = 'test_get_without_compression\\data'

        cache = SimpleFileCache(cache_root='reservoir_cache_integration', compressionOn=False)

        cacheddata = self.cache.get(
            lambda: self._data,
            self._data_to_csv,
            self._csv_to_data,
            key)

        self.assertTrue(cacheddata.equals(self._data))

    def test_get_read_performance(self):

        key = 'test_perf\\data'

        cache = SimpleFileCache(cache_root='reservoir_cache_integration', compressionOn=True)

        # ensure data is cached
        cacheddata = self.cache.get(
            lambda: self._data,
            self._data_to_csv,
            self._csv_to_data,
            key)

        iterations = 10
        start = timer()
        for i in range(iterations):
            self.cache.get(
                lambda: self._data,
                self._data_to_csv,
                self._csv_to_data,
                key)
        stop = timer()

        print('Average cache read: {}'.format((stop - start) / iterations))


    def _data_to_csv(self, data, stream):
         data.to_csv(stream, header=False)

    def _csv_to_data(self, stream):
        return pd.read_csv(stream, header=None, names=['index', 'values'], index_col=0)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(__file__).setLevel(logging.DEBUG)
    logging.getLogger("datareservoirio.storage").setLevel(logging.DEBUG)
    unittest.main()