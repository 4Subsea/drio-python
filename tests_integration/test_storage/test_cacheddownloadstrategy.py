import unittest
import logging
import sys
import pandas as pd
from mock import patch
from timeit import timeit

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import Storage, SimpleFileCache, CachedDownloadStrategy

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


log = logging.getLogger(__file__)
TIMESERIESID = '06C0AD81-3E81-406F-9DB0-EFD5114DD5E0'


class Test_CachedDownloadStrategy(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

    def setUp(self):
        self.timeseries_api = TimeSeriesAPI()
        self._cache = SimpleFileCache(cache_root='./_cache/test_cacheddownloadstrategy')

    def test_get_with_msgpack_format(self):
        strategy = CachedDownloadStrategy(cache=self._cache, format='msgpack')
        chunks = self.timeseries_api.download_days(
            self.auth.token, TIMESERIESID,
            1513468800000000000, 1513814500000000000)
        iterations=100

        usedtime = timeit(stmt=lambda: strategy.get(chunks), number=iterations)

        print('Average cache read with msgpack: {}'.format(usedtime/iterations))

    def test_get_with_csv_format(self):
        strategy = CachedDownloadStrategy(cache=self._cache, format='csv')
        chunks = self.timeseries_api.download_days(
            self.auth.token, TIMESERIESID,
            1513468800000000000, 1513814500000000000)
        iterations=100

        usedtime = timeit(stmt=lambda: strategy.get(chunks), number=iterations)

        print('Average cache read with csv: {}'.format(usedtime/iterations))

if __name__ == '__main__':
    unittest.main()