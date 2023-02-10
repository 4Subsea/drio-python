import time
import unittest

import numpy as np
import pandas as pd
import requests
from requests.exceptions import HTTPError

from datareservoirio.authenticate import ClientAuthenticator
from datareservoirio.rest_api import FilesAPI, MetadataAPI, TimeSeriesAPI
from datareservoirio.storage import DirectUpload
from tests_integration._auth import CLIENT


class Test_TimeSeriesApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        cls.metaapi = MetadataAPI(session=cls.auth)
        files_api = FilesAPI(session=cls.auth)

        uploader = DirectUpload()

        df_1 = pd.DataFrame({"values": np.arange(100.0)}, index=np.arange(0, 100))
        df_2 = pd.DataFrame({"values": np.arange(100.0)}, index=np.arange(50, 150))
        df_3 = pd.DataFrame({"values": np.arange(50.0)}, index=np.arange(125, 175))

        df_list = [df_1, df_2, df_3]
        cls.token_fileid = []
        for df in df_list:
            upload_params = files_api.upload()
            token_fileid = upload_params["FileId"]

            uploader.put(upload_params, df)

            files_api.commit(upload_params["FileId"])

            counter = 0
            response = files_api.status(upload_params["FileId"])
            while response["State"] != "Ready" and counter < 5:
                time.sleep(5)
                response = files_api.status(upload_params["FileId"])
                counter += 1

            cls.token_fileid.append(token_fileid)

        cls.meta_1 = {
            "Namespace": "namespace_string_1",
            "Key": "key_string_1",
            "Value": {"Ding": "Dong"},
        }

        meta_1_value = {"Value": {"Ding": "Dong"}}
        cls.meta_response = cls.metaapi.put(
            "namespace_string_1", "key_string_1", meta_1_value
        )

    @classmethod
    def tearDownClass(cls):
        cls.metaapi.delete(cls.meta_response["Id"])
        cls.auth.close()

    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.api = TimeSeriesAPI(session=self.auth)

    def tearDown(self):
        self.auth.close()

    def test_get_with_nonexisting_timeseries(self):
        with self.assertRaises(requests.exceptions.HTTPError) as e:
            self.api.download_days("05cbaeda-a5ad-430e-b640-46023488258b", -1000, 1000)
            self.assertEqual(e.exception.response.status_code, 404)

    def test_create_delete(self):
        response = self.api.create()
        info = self.api.info(response["TimeSeriesId"])

        self.assertEqual(response["TimeSeriesId"], info["TimeSeriesId"])
        self.assertEqual(None, info["TimeOfFirstSample"])
        self.assertEqual(None, info["TimeOfLastSample"])

        self.api.download_days(response["TimeSeriesId"], -1000, 1000)
        self.api.delete(response["TimeSeriesId"])

    def test_create_data_delete(self):
        response = self.api.create_with_data(self.token_fileid[0])

        info = self.api.info(response["TimeSeriesId"])

        self.assertEqual(0, response["TimeOfFirstSample"])
        self.assertEqual(info["TimeOfFirstSample"], response["TimeOfFirstSample"])

        self.assertEqual(99, response["TimeOfLastSample"])
        self.assertEqual(info["TimeOfLastSample"], response["TimeOfLastSample"])

        self.api.download_days(response["TimeSeriesId"], -1000, 1000)
        self.api.delete(response["TimeSeriesId"])

    def test_delete(self):
        response = self.api.create(self.token_fileid[0])
        self.api.info(response["TimeSeriesId"])
        self.api.delete(response["TimeSeriesId"])

        with self.assertRaises(HTTPError):
            self.api.info(response["TimeSeriesId"])

    def test_create_add_overlap_data_delete(self):
        response = self.api.create_with_data(self.token_fileid[0])

        response = self.api.add(response["TimeSeriesId"], self.token_fileid[1])
        response = self.api.add(response["TimeSeriesId"], self.token_fileid[2])

        info = self.api.info(response["TimeSeriesId"])

        self.assertEqual(0, info["TimeOfFirstSample"])
        self.assertEqual(174, info["TimeOfLastSample"])

        self.api.download_days(response["TimeSeriesId"], -1000, 1000)
        self.api.delete(response["TimeSeriesId"])

    def test_create_add_nooverlap_data_delete(self):
        response = self.api.create_with_data(self.token_fileid[0])
        response = self.api.add(response["TimeSeriesId"], self.token_fileid[2])

        info = self.api.info(response["TimeSeriesId"])

        self.assertEqual(0, info["TimeOfFirstSample"])
        self.assertEqual(174, info["TimeOfLastSample"])

        self.api.download_days(response["TimeSeriesId"], -1000, 1000)
        self.api.delete(response["TimeSeriesId"])

    def test_attach_detach_meta(self):
        response = self.api.create()
        meta_id = self.meta_response["Id"]

        self.api.attach_metadata(response["TimeSeriesId"], metadata_id_list=[meta_id])

        response = self.api.info(response["TimeSeriesId"])
        self.assertEqual(
            len([m for m in response["Metadata"] if m["Id"] == meta_id]), 1
        )

        self.api.detach_metadata(response["TimeSeriesId"], metadata_id_list=[meta_id])

        response = self.api.info(response["TimeSeriesId"])
        self.assertEqual(
            len([m for m in response["Metadata"] if m["Id"] == meta_id]), 0
        )

        self.api.delete(response["TimeSeriesId"])


if __name__ == "__main__":
    unittest.main()
