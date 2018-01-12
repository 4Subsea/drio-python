import logging
import pprint
import time
import unittest
from mock import patch

import numpy as np
import pandas as pd
from requests.exceptions import HTTPError

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api import FilesAPI, TimeSeriesAPI

from tests_integration._auth import USER

datareservoirio.set_log_level('DEBUG')
datareservoirio.logger.addHandler(logging.FileHandler('upload.log'))

datareservoirio.globalsettings.environment.set_test()


class Test_ClientUploadDownload(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

    def setUp(self):
        self.client = datareservoirio.Client(self.auth)

    def test_upload_download_1day(self):
        self._test_upload_download_days(1)

    def test_upload_download_10day(self):
        self._test_upload_download_days(10)

    def test_upload_download_100day(self):
        self._test_upload_download_days(100)

    def _test_upload_download_days(self, days):
        self.df = pd.Series(np.arange(days*864000.),
                            index=np.arange(0, days*864000))
        self.df.index.name = 'index'
        self.df.name = 'values'

        self.response = self.client.create(self.df)

        info = self.client.info(self.response['TimeSeriesId'])

        # Get entire file
        self.client.get(self.response['TimeSeriesId'])

        # Get first 10% of file
        self._percent_download(0.10, 0.10)

        # Get middle 10% of file
        self._percent_download(0.50, 0.10)

        # Get last 10% of file
        self._percent_download(0.90, 0.10)

        self.client.delete(self.response['TimeSeriesId'])

    def _percent_download(self, percent_start, percent_delta):
        start = self._get_index_percent(self.df, percent_start)
        end = self._get_index_percent(self.df, percent_start+percent_delta)
        data_recieved = self.client.get(self.response['TimeSeriesId'],
                                        start=start, end=end)
        pd.util.testing.assert_series_equal(self.df[start:end+1],
                                            data_recieved)

    @staticmethod
    def _get_index_percent(df, percent):
        length = len(df.index) - 1
        index = int(percent*length)
        return df.index[index]


if __name__ == '__main__':
    unittest.main()
