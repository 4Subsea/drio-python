import base64
import unittest

import numpy as np
import pandas as pd

from datareservoirio.storage import Storage

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock


class Test_Storage(unittest.TestCase):
    def setUp(self):
        self._auth = Mock()
        self._timeseries_api = Mock()
        self._files_api = Mock()
        self.downloader = Mock()
        self.uploader = Mock()

        self.tid = 'abc-123-xyz'

        self.storage = Storage(
            self._auth,
            self._timeseries_api,
            self._files_api,
            downloadStrategy=self.downloader,
            uploadStrategy=self.uploader)

    def test_constructor(self):
        storage = Storage(self._auth, self._timeseries_api, self._files_api)

    def test_get(self):
        data = pd.DataFrame(data={'values': [1, 2, 3, 4]}, index=[1, 2, 3, 4], columns=['index', 'values'])
        self.downloader.get.return_value = data

        df = self.storage.get(self.tid, 1, 10)

        self.assertTrue(df.equals(data))

    def test_put(self):
        self._files_api.upload.return_value = {'FileId':'42'}

        fileId = self.storage.put('data')

        self.assertEquals(fileId, '42')


if __name__ == '__main__':
    unittest.main()