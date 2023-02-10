import logging
import time
import unittest

import numpy as np
import pandas as pd

from datareservoirio.authenticate import ClientAuthenticator
from datareservoirio.rest_api import FilesAPI, TimeSeriesAPI
from datareservoirio.storage import BaseDownloader, DirectDownload, DirectUpload
from tests_integration._auth import CLIENT

log = logging.getLogger(__file__)


class Test_DirectDownload(unittest.TestCase):
    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.timeseries_api = TimeSeriesAPI(session=self.auth)

        self.strategy = BaseDownloader(DirectDownload())
        self.files_api = FilesAPI(session=self.auth)

        uploader = DirectUpload()

        df = pd.DataFrame({"values": np.arange(100.0)}, index=np.arange(0, 100))

        upload_params = self.files_api.upload()
        self.token_fileid = upload_params["FileId"]

        uploader.put(upload_params, df)

        self.files_api.commit(upload_params["FileId"])

    def tearDown(self):
        self.auth.close()

    def test_get(self):
        response = self.timeseries_api.create()
        myfileid = self.token_fileid

        response = self.timeseries_api.add(response["TimeSeriesId"], myfileid)

        chunks = self.timeseries_api.download_days(response["TimeSeriesId"], 0, 100)

        series = self.strategy.get(chunks)

        self.assertIsNotNone(series)
        self.assertFalse(series.empty)
        log.debug(series)


if __name__ == "__main__":
    unittest.main()
