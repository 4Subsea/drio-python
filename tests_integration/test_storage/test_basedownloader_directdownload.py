import logging
import unittest

import requests

from datareservoirio.authenticate import ClientAuthenticator
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import BaseDownloader, DirectDownload
from tests_integration._auth import CLIENT

log = logging.getLogger(__file__)
TIMESERIESID = "06C0AD81-3E81-406F-9DB0-EFD5114DD5E0"


class Test_DirectDownload(unittest.TestCase):
    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.timeseries_api = TimeSeriesAPI(session=self.auth)

        self._session = requests.Session()
        self.strategy = BaseDownloader(DirectDownload(session=self._session))

    def tearDown(self):
        self.auth.close()
        self._session.close()

    def test_get(self):
        chunks = self.timeseries_api.download_days(
            TIMESERIESID, 1513468800000000000, 1513814400000000000
        )

        series = self.strategy.get(chunks)

        self.assertIsNotNone(series)
        self.assertFalse(series.empty)
        log.debug(series)


if __name__ == "__main__":
    unittest.main()
