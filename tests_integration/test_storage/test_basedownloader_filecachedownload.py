import logging
import unittest
from functools import partial
from timeit import timeit

import requests

from datareservoirio.authenticate import ClientAuthenticator
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import BaseDownloader, FileCacheDownload
from tests_integration._auth import CLIENT

log = logging.getLogger(__file__)
TIMESERIESID = "06C0AD81-3E81-406F-9DB0-EFD5114DD5E0"


class Test_CachedDownloadStrategy(unittest.TestCase):
    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.timeseries_api = TimeSeriesAPI(session=self.auth)
        self._cache_root = "./_cache/test_filecachedownload"

        self._session = requests.Session()

    def tearDown(self):
        self.auth.close()
        self._session.close()

    def test_get_with_parquet_format(self):
        strategy = BaseDownloader(
            FileCacheDownload(
                cache_root=self._cache_root, format_="parquet", session=self._session
            )
        )

        chunks = self.timeseries_api.download_days(
            TIMESERIESID, 1513468800000000000, 1513814500000000000
        )
        iterations = 100

        usedtime = timeit(stmt=partial(strategy.get, chunks), number=iterations)

        print("Average cache read with parquet: {}".format(usedtime / iterations))

    def test_get_with_csv_format(self):
        strategy = BaseDownloader(
            FileCacheDownload(
                cache_root=self._cache_root, format_="csv", session=self._session
            )
        )

        chunks = self.timeseries_api.download_days(
            TIMESERIESID, 1513468800000000000, 1513814500000000000
        )
        iterations = 100

        usedtime = timeit(stmt=lambda: strategy.get(chunks), number=iterations)

        print("Average cache read with csv: {}".format(usedtime / iterations))


if __name__ == "__main__":
    unittest.main()
