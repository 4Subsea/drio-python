import unittest
import logging
import os
import numpy as np
import pandas as pd
import codecs
from random import random
from timeit import timeit

from datareservoirio.storage import SimpleFileCache

log = logging.getLogger(__file__)

_CACHE_ROOT = './_cache/test_simplefilecache'


class Test_SimpleFileCache(unittest.TestCase):
    def setUp(self):

        self._data = pd.Series(np.arange(100000.), index=np.arange(0, 100000))
        self._data.index.name = 'index'
        self._data.name = 'values'

        self.cache = SimpleFileCache(cache_root=_CACHE_ROOT)

    def test_get_when_key_changes_cache_is_updated(self):
        rows = [(0, 1.0), (1, 2.0), (2, 3.0)]
        newdata = pd.DataFrame.from_records(rows, columns=['index', 'values'])
        key = 'test_get_when_key_changes_cache_is_updated\\data\\{}'

        self.cache.get(
            lambda: self._data,
            self._data_to_csv,
            self._csv_to_data,
            key.format(1))

        self.cache.reset_cache()

        newcacheddata = self.cache.get(
            lambda: newdata,
            self._data_to_csv,
            self._csv_to_data,
            key.format(2))

        self.assertTrue(newcacheddata.equals(newdata))
        self.assertFalse(newcacheddata.equals(self._data))

    def test_get_when_serializer_throws_does_not_create_cache_file(self):
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

    def test_get_read_performance(self):

        key = 'test_get_read_performance\\data\\{}'

        cache = SimpleFileCache(cache_root=_CACHE_ROOT, max_size=10)

        # ensure data is cached
        cache.get(
            lambda: self._data,
            self._data_to_csv,
            self._csv_to_data,
            key)

        def _action():
            cache.get(
                lambda: self._data,
                self._data_to_csv,
                self._csv_to_data,
                key.format(random()*1000))

        iterations = 5
        usedtime = timeit(stmt=_action, number=iterations)

        print('Average cache read with cache-write: {}'.format(usedtime / iterations))

    def _data_to_csv(self, data, stream):
        with codecs.getwriter('utf-8')(stream) as sw:
            data.to_csv(sw, header=False, encoding='ascii')

    def _csv_to_data(self, stream):
        with codecs.getreader('utf-8')(stream) as sr:
            return pd.read_csv(sr, header=None, names=['index', 'values'], index_col=0, encoding='ascii')


if __name__ == '__main__':
    logger = logging.getLogger("datareservoirio")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    unittest.main()
