import sys
import time
import unittest

import numpy as np
import pandas as pd
from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api.files import FilesAPI

if sys.version_info.major == 3:
    from io import StringIO
elif sys.version_info.major == 2:
    from cStringIO import StringIO

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


class Test_FilesApi(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

    def setUp(self):
        self.api = FilesAPI()

    def test_ping(self):
        self.api.ping(self.auth.token)

    def test_upload_df_cycle(self):
        upload_params = self.api.upload(self.auth.token)
        print upload_params
        uploader = self.api.transfer_service(upload_params)

        df = pd.Series(np.arange(1e3))
        df.index.name = 'time'
        df.name = 'values'

        uploader.create_blob_from_series(df)

        self.api.commit(self.auth.token, upload_params['FileId'])

        counter = 0
        response = self.api.status(self.auth.token, upload_params['FileId'])
        while response['State'] != 'Ready' and counter < 15:
            print(counter, response['State'])
            time.sleep(5)
            response = self.api.status(
                self.auth.token, upload_params['FileId'])
            counter += 1

        response = self.api.bytes(self.auth.token, upload_params['FileId'])
        response_txt = StringIO(response)
        df_recieved = pd.read_csv(response_txt, header=None,
                                  names=['time', 'values'], index_col=0)
        response_txt.close()

        pd.util.testing.assert_series_equal(df, df_recieved['values'])


if __name__ == '__main__':
    unittest.main()
