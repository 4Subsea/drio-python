import time
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import requests

import datareservoirio
from datareservoirio.authenticate import UserCredentials
from datareservoirio.rest_api.files import FilesAPI
from datareservoirio.storage.uploadstrategy import UploadStrategy
from tests_integration._auth import USER


class Test_FilesApi(unittest.TestCase):

    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUp(self, mock_pass):
        self.auth = UserCredentials(USER.NAME, auth_force=True)
        self.api = FilesAPI(self.auth)

    def tearDown(self):
        self.auth.close()

    def test_ping(self):
        self.api.ping()

    def test_upload_df_cycle(self):
        upload_params = self.api.upload()
        file_id = upload_params['FileId']

        with requests.Session() as s:
            uploader = UploadStrategy(session=s)

            df = pd.Series(np.arange(1e3))
            df.index.name = 'time'
            df.name = 'values'

            uploader.put(upload_params, df)

            self.api.commit(file_id)

            counter = 0
            response = self.api.status(file_id)
            while response['State'] != 'Ready' and counter < 15:
                time.sleep(5)
                response = self.api.status(file_id)
                counter += 1

            self.assertLess(counter, 15, 'Processing did not complete with Ready status')


if __name__ == '__main__':
    unittest.main()
