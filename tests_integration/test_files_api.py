import time
import unittest

import numpy as np
import pandas as pd
import requests

from datareservoirio.authenticate import ClientAuthenticator
from datareservoirio.rest_api.files import FilesAPI
from datareservoirio.storage import DirectUpload
from tests_integration._auth import CLIENT


class Test_FilesApi(unittest.TestCase):
    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.api = FilesAPI(self.auth)

    def tearDown(self):
        self.auth.close()

    def test_ping(self):
        self.api.ping()


    def test_upload_df_cycle(self):
        upload_params = self.api.upload()
        file_id = upload_params["FileId"]

        with requests.Session() as s:
            uploader = DirectUpload()
            # Feiler her med meldingen "__init__() got an unexpected keyword argument "session" -> pga omskriving av AzureBlobService? 

            df = pd.DataFrame({"values": np.arange(1e3)})
            df.index.name = "time"
            df.name = "values"

            uploader.put(upload_params, df)

            self.api.commit(file_id)

            counter = 0
            response = self.api.status(file_id)
            while response["State"] != "Ready" and counter < 15:
                time.sleep(5)
                response = self.api.status(file_id)
                counter += 1

            self.assertLess(
                counter, 15, "Processing did not complete with Ready status"
            )



if __name__ == "__main__":
    unittest.main()
