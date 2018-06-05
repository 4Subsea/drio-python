import sys
import time
import unittest
import numpy as np
import pandas as pd
import requests

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api.files import FilesAPI
from datareservoirio.storage.uploadstrategy import UploadStrategy

from tests_integration._auth import USER

if sys.version_info.major == 3:
    from io import StringIO
elif sys.version_info.major == 2:
    from cStringIO import StringIO

datareservoirio.globalsettings.environment.set_test()


class Test_FilesApi(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

    def setUp(self):
        self._session = requests.Session()
        self.api = FilesAPI(session=self._session)

    def tearDown(self):
        self._session.close()

    def test_ping(self):
        self.api.ping(self.auth.token)

    def test_upload_df_cycle(self):
        upload_params = self.api.upload(self.auth.token)
        file_id = upload_params['FileId']
        print(upload_params)

        with requests.Session() as s:
            uploader = UploadStrategy(session=s)

            df = pd.Series(np.arange(1e3))
            df.index.name = 'time'
            df.name = 'values'

            uploader.put(upload_params, df)

            self.api.commit(self.auth.token, file_id)

            counter = 0
            response = self.api.status(self.auth.token, file_id)
            while response['State'] != 'Ready' and counter < 15:
                print(counter, response['State'])
                time.sleep(5)
                response = self.api.status(
                    self.auth.token, file_id)
                counter += 1

            response = self.api.bytes(self.auth.token, file_id)
            response_txt = StringIO(response)
            df_recieved = pd.read_csv(response_txt, header=None,
                                      names=['time', 'values'], index_col=0)
            response_txt.close()

            pd.util.testing.assert_series_equal(df, df_recieved['values'])


if __name__ == '__main__':
    unittest.main()
