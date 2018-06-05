import unittest
import logging
import requests

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import AlwaysDownloadStrategy

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


log = logging.getLogger(__file__)
TIMESERIESID = '06C0AD81-3E81-406F-9DB0-EFD5114DD5E0'


class Test_AlwaysDownloadStrategy(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

    def setUp(self):
        self._session = requests.Session()
        self.timeseries_api = TimeSeriesAPI(session=self._session)
        self.strategy = AlwaysDownloadStrategy(session=self._session)

    def tearDown(self):
        self._session.close()

    def test_get(self):
        chunks = self.timeseries_api.download_days(
            self.auth.token, TIMESERIESID,
            1513468800000000000, 1513814400000000000)

        series = self.strategy.get(chunks)

        self.assertIsNotNone(series)
        self.assertFalse(series.empty)
        log.debug(series)


if __name__ == '__main__':
    logger = logging.getLogger("datareservoirio")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    unittest.main()
