import unittest
import logging
import sys
import pandas as pd
from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.storage import Storage, CachedDownloadStrategy

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


log = logging.getLogger(__file__)
TIMESERIESID = '06C0AD81-3E81-406F-9DB0-EFD5114DD5E0'

class Test_Storage(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)
        
    def setUp(self):
        self.storage = Storage()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger(__file__).setLevel(logging.DEBUG)
    logging.getLogger("datareservoirio.storage").setLevel(logging.DEBUG)
    logging.getLogger("datareservoirio.storage_engine").setLevel(logging.DEBUG)
    unittest.main()