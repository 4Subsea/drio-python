import base64
import unittest

import numpy as np
import pandas as pd

from datareservoirio.storage import CachedDownloadStrategy, SimpleFileCache

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock


class Test_CachedDownloadStrategy(unittest.TestCase):
    def setUp(self):
        self._blob_to_series_patch = patch('datareservoirio.storage.CachedDownloadStrategy._blob_to_series')
        self._blob_to_series = self._blob_to_series_patch.start()
        self.addCleanup(self._unpatch)
        self._cache = Mock()

        self._df = pd.DataFrame(data=[{'1':42},{'2':32}], columns=['index', 'values'])
        self._files = {
            "Files": [
                {
                "Index": 0,
                "FileId": '42',
                "Chunks": [
                    {
                    "Account": "acc",
                    "SasKey": "sas",
                    "Container": "cnt",
                    "Path": "pth",
                    "Endpoint": "ep",
                    "ContentMd5": "md5"
                    }
                ]
                }
            ]
        }

        self._blob_to_series.return_value = self._df
        self._cache.get.return_value = self._df

        self._dl = CachedDownloadStrategy(cache=self._cache)

    def _unpatch(self):
        self._blob_to_series_patch.stop()

    def test_init_with_invalid_format_raises_exception(self):
        with self.assertRaises(ValueError):
            CachedDownloadStrategy(self._cache, format='bogusformat')

    def test_init_default_uses_simplefilecache(self):
        dl = CachedDownloadStrategy()
        self.assertIsInstance(dl._cache, SimpleFileCache)

    def test_init_with_cache(self):
        dl = CachedDownloadStrategy(cache=self._cache)
        self.assertIsInstance(dl._cache, Mock)

    def test_init_default_serialization_is_msgpack(self):
        dl = CachedDownloadStrategy(cache=self._cache)

        sr = dl.get(self._files)

        calls = self._cache.get.call_args[0]
        self.assertIn('bWQ1', calls)
        self.assertIn('mp', calls)

    def test_init_msgpack_serialization_md5file_have_mp_postfix(self):
        dl = CachedDownloadStrategy(cache=self._cache, format='msgpack')

        sr = dl.get(self._files)

        calls = self._cache.get.call_args[0]
        self.assertIn('bWQ1', calls)
        self.assertIn('mp', calls)

    def test_init_csv_serialization_md5file_have_csv_postfix(self):
        dl = CachedDownloadStrategy(cache=self._cache, format='csv')

        sr = dl.get(self._files)

        calls = self._cache.get.call_args[0]
        self.assertIn('bWQ1', calls)
        self.assertIn('csv', calls)

    def test_get(self):
        sr = self._dl.get(self._files)

        self.assertTrue(sr.equals(self._df))


if __name__ == '__main__':
    unittest.main()