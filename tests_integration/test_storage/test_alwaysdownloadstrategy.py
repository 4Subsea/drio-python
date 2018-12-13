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

    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUp(self, mock_input):
        self.auth = Authenticator(USER.NAME, auth_force=True)
        self.timeseries_api = TimeSeriesAPI(session=self.auth)

        self._session = requests.Session()
        self.strategy = AlwaysDownloadStrategy(session=self._session)

    def tearDown(self):
        self.auth.close()
        self._session.close()

    def test_get(self):
        chunks = self.timeseries_api.download_days(
            TIMESERIESID, 1513468800000000000, 1513814400000000000)

        series = self.strategy.get(chunks)

        self.assertIsNotNone(series)
        self.assertFalse(series.empty)
        log.debug(series)


if __name__ == '__main__':
    unittest.main()
