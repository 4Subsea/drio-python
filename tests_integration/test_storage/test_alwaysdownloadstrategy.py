import unittest
import logging
import sys
import pandas as pd
from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import Storage, AlwaysDownloadStrategy

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
        self.timeseries_api = TimeSeriesAPI()
        self.strategy = AlwaysDownloadStrategy()

    def test_get(self):
        chunks = self.timeseries_api.download_days(
            self.auth.token, TIMESERIESID,
            1513468800000000000, 1513814400000000000)

        series = self.strategy.get(chunks)

        self.assertIsNotNone(series)
        self.assertFalse(series.empty)
        log.debug(series)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(__file__).setLevel(logging.DEBUG)
    logging.getLogger("datareservoirio.storage").setLevel(logging.DEBUG)
    logging.getLogger("datareservoirio.storage_engine").setLevel(logging.DEBUG)
    unittest.main()