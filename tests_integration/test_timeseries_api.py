import pprint
import time
import unittest
from mock import patch

import numpy as np
import pandas as pd

import timeseriesclient
from timeseriesclient.authenticate import Authenticator
from timeseriesclient.rest_api import FilesApi, TimeSeriesApi

timeseriesclient.globalsettings.environment.set_qa()

USERNAME = 'ace@4subsea.com'
PASSWORD = '#bmE378dt!'

#USERNAME = 'reservoir-integrationtest@4subsea.onmicrosoft.com'
#PASSWORD = 'LnqABDrHLYceXLWC7YFhbVAq8dqvPeRAMzbTYKGn'


class Test_TimeSeriesApi(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USERNAME)
        
        files_api = FilesApi()

        df_1 = pd.DataFrame({'a': np.arange(100.)}, index=np.arange(0, 100))
        df_2 = pd.DataFrame({'a': np.arange(100.)}, index=np.arange(50, 150))
        df_3 = pd.DataFrame({'a': np.arange(50.)}, index=np.arange(125, 175))

        df_list = [df_1, df_2, df_3]
        cls.token_fileid = []
        for df in df_list:
            upload_params = files_api.upload(cls.auth.token)
            token_fileid = (cls.auth.token, upload_params['FileId'])
            uploader = files_api.upload_service(upload_params)

            uploader.create_blob_from_df(df)

            files_api.commit(cls.auth.token, upload_params['FileId'])

            counter = 0
            response = files_api.status(cls.auth.token, upload_params['FileId'])
            while response['State'] != 'Ready' and counter < 5:
                time.sleep(5)
                response = files_api.status(cls.auth.token, upload_params['FileId'])
                counter += 1

            cls.token_fileid.append(token_fileid)

    def setUp(self):
        self.api = TimeSeriesApi()

    def test_create_data_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        info = self.api.info(*token_tsid)


        self.assertEqual(0, response['TimeOfFirstSample'])
        self.assertEqual(info['TimeOfFirstSample'], response['TimeOfFirstSample'])

        self.assertEqual(99, response['TimeOfLastSample'])
        self.assertEqual(info['TimeOfLastSample'], response['TimeOfLastSample'])

        data = self.api.data(self.auth.token, response['TimeSeriesId'], -1000, 1000)
        self.api.delete(*token_tsid)

    def test_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])

        info_pre = self.api.info(*token_tsid)
        self.api.delete(*token_tsid)

        with self.assertRaises(ValueError):
            info_post = self.api.info(*token_tsid)

    def test_list(self):
        response = self.api.list(self.token_fileid[0][0])
        self.assertTrue(isinstance(response, list))

    def test_create_add_overlap_data_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        response = self.api.add(self.auth.token, response['TimeSeriesId'], self.token_fileid[1][1])
        response = self.api.add(self.auth.token, response['TimeSeriesId'], self.token_fileid[2][1])

        info = self.api.info(*token_tsid)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data = self.api.data(self.auth.token, response['TimeSeriesId'], -1000, 1000)
        self.api.delete(*token_tsid)
        print info

    def test_create_add_nooverlap_data_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        response = self.api.add(self.auth.token, response['TimeSeriesId'], self.token_fileid[2][1])

        info = self.api.info(*token_tsid)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data = self.api.data(self.auth.token, response['TimeSeriesId'], -1000, 1000)
        self.api.delete(*token_tsid)
        print info


if __name__ == '__main__':
    unittest.main()
