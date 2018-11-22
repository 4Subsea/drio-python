import unittest
import pandas as pd

from datareservoirio.storage import Storage

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock


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
            downloader=self.downloader,
            uploader=self.uploader)

    def test_get(self):
        data = pd.Series([1, 2, 3, 4], index=[1, 2, 3, 4])
        self.downloader.get.return_value = data

        series = self.storage.get(self.tid, 1, 10)

        self.assertTrue(series.equals(data))

    def test_put(self):
        self._files_api.upload.return_value = {'FileId': '42'}

        fileId = self.storage.put('data')

        self.assertEqual(fileId, '42')

    def test__create_series(self):
        series = pd.Series(data=[0, 2, 4, 8, 16, 32, 64],
                           index=[0, 2, 4, 6, 8, 10, 12])
        series_returned = self.storage._create_series(series, 4, 10)
        series_expected = pd.Series(data=[4, 8, 16, 32],
                                    index=[4, 6, 8, 10])
        pd.testing.assert_series_equal(series_returned, series_expected)
        self.assertFalse(series_returned._is_view)  # opps! private api...


if __name__ == '__main__':
    unittest.main()
