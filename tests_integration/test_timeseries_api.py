import pprint
import time
import unittest

import numpy as np
import pandas as pd
from mock import patch
from requests.exceptions import HTTPError

import datareservoirio
from datareservoirio.authenticate import Authenticator
from datareservoirio.rest_api import FilesAPI, TimeSeriesAPI, MetadataAPI
from datareservoirio.storage.uploadstrategy import UploadStrategy

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


class Test_TimeSeriesApi(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)
        cls.metaapi = MetadataAPI()

        files_api = FilesAPI()
        uploader = UploadStrategy()

        df_1 = pd.Series(np.arange(100.), index=np.arange(0, 100))
        df_2 = pd.Series(np.arange(100.), index=np.arange(50, 150))
        df_3 = pd.Series(np.arange(50.), index=np.arange(125, 175))

        df_list = [df_1, df_2, df_3]
        cls.token_fileid = []
        for df in df_list:
            upload_params = files_api.upload(cls.auth.token)
            token_fileid = (cls.auth.token, upload_params['FileId'])

            uploader.put(upload_params, df)

            files_api.commit(cls.auth.token, upload_params['FileId'])

            counter = 0
            response = files_api.status(
                cls.auth.token, upload_params['FileId'])
            while response['State'] != 'Ready' and counter < 5:
                time.sleep(5)
                response = files_api.status(
                    cls.auth.token, upload_params['FileId'])
                counter += 1

            cls.token_fileid.append(token_fileid)

        cls.meta_1 = {
            "Namespace": "namespace_string_1",
            "Key": "key_string_1",
            "Value": {
                "Ding": "Dong"
            }
        }

        cls.metaapi = MetadataAPI()
        cls.meta_respons = cls.metaapi.create(cls.auth.token, cls.meta_1)

    @classmethod
    def tearDownClass(cls):
        cls.metaapi.delete(cls.auth.token, cls.meta_respons['Id'])

    def setUp(self):
        self.api = TimeSeriesAPI()

    def test_create_data_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        info = self.api.info(*token_tsid)

        self.assertEqual(0, response['TimeOfFirstSample'])
        self.assertEqual(info['TimeOfFirstSample'],
                         response['TimeOfFirstSample'])

        self.assertEqual(99, response['TimeOfLastSample'])
        self.assertEqual(info['TimeOfLastSample'],
                         response['TimeOfLastSample'])

        data_files = self.api.download_days(self.auth.token, response[
                             'TimeSeriesId'], -1000, 1000)
        self.api.delete(*token_tsid)

    def test_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])

        info_pre = self.api.info(*token_tsid)
        self.api.delete(*token_tsid)

        with self.assertRaises(HTTPError):
            info_post = self.api.info(*token_tsid)

    def test_list(self):
        response = self.api.list(self.token_fileid[0][0])
        self.assertTrue(isinstance(response, list))

    def test_create_add_overlap_data_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        response = self.api.add(self.auth.token, response[
                                'TimeSeriesId'], self.token_fileid[1][1])
        response = self.api.add(self.auth.token, response[
                                'TimeSeriesId'], self.token_fileid[2][1])

        info = self.api.info(*token_tsid)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_days = self.api.download_days(self.auth.token, response[
                             'TimeSeriesId'], -1000, 1000)
        self.api.delete(*token_tsid)
        print info

    def test_create_add_nooverlap_data_delete(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        response = self.api.add(self.auth.token, response[
                                'TimeSeriesId'], self.token_fileid[2][1])

        info = self.api.info(*token_tsid)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_days = self.api.download_days(self.auth.token, response[
                             'TimeSeriesId'], -1000, 1000)
        self.api.delete(*token_tsid)
        print info

    def test_attach_detach_meta(self):
        response = self.api.create(*self.token_fileid[0])
        token_tsid = (self.auth.token, response['TimeSeriesId'])
        print response
        self.api.attach_metadata(*token_tsid,
                                 metadata_id_list=[self.meta_respons['Id']])

        response = self.api.info(*token_tsid)
        self.assertEqual(response['Metadata'][0]['Id'],
                         self.meta_respons['Id'])

        self.api.detach_metadata(*token_tsid,
                                 metadata_id_list=[self.meta_respons['Id']])

        response = self.api.info(*token_tsid)
        self.assertListEqual(response['Metadata'], [])

        self.api.delete(*token_tsid)


if __name__ == '__main__':
    unittest.main()
