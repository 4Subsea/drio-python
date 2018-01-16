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

        self.download = CachedDownloadStrategy(cache=self._cache)

    def _unpatch(self):
        self._blob_to_series_patch.stop()

    def test_init_default_uses_simplefilecache(self):
        download = CachedDownloadStrategy()
        self.assertIsInstance(download._cache, SimpleFileCache)

    def test_init_with_cache(self):
        download = CachedDownloadStrategy(cache=Mock())
        self.assertIsInstance(download._cache, Mock)

    def test_get(self):
        filesResponse = {
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

        storedDataframe = pd.DataFrame(data=[{'1':42},{'2':32}], columns=['index', 'values'])
        self._blob_to_series.return_value = storedDataframe
        self._cache.get.return_value = storedDataframe

        sr = self.download.get(filesResponse)

        self.assertTrue(sr.equals(storedDataframe))


if __name__ == '__main__':
    unittest.main()