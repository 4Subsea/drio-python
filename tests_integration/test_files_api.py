import time
import unittest
from mock import patch

import numpy as np
import pandas as pd

import timeseriesclient
from timeseriesclient.authenticate import Authenticator
from timeseriesclient.rest_api.files_api import FilesApi

timeseriesclient.globalsettings.environment.set_qa()

USERNAME = 'reservoir-integrationtest@4subsea.com'
PASSWORD = 'qz9uVgNhANncz9Jp'


class Test_FilesApi(unittest.TestCase):
    @classmethod
    @patch('getpass.getpass', return_value=PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USERNAME)

    def setUp(self):
        self.api = FilesApi()

    def test_ping(self):
        self.api.ping(self.auth.token)

    def test_upload_df_cycle(self):
        upload_params = self.api.upload(self.auth.token)
        print upload_params
        uploader = self.api.upload_service(upload_params)

        df = pd.DataFrame({'a': np.arange(1e3)})

        uploader.create_blob_from_df(df)

        self.api.commit(self.auth.token, upload_params['FileId'])

        counter = 0
        response = self.api.status(self.auth.token, upload_params['FileId'])
        while response['State'] != 'Ready' and counter < 15:
            print(counter, response['State'])
            time.sleep(5)
            response = self.api.status(self.auth.token, upload_params['FileId'])
            counter += 1


if __name__ == '__main__':
    unittest.main()
