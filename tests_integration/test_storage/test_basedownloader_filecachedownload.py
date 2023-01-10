import logging
import unittest
from functools import partial
from timeit import timeit

import pandas as pd
import numpy as np


from datareservoirio.authenticate import ClientAuthenticator
from datareservoirio.rest_api import TimeSeriesAPI, FilesAPI
from datareservoirio.storage import BaseDownloader, FileCacheDownload, DirectUpload
from tests_integration._auth import CLIENT

log = logging.getLogger(__file__)
TIMESERIESID = "06C0AD81-3E81-406F-9DB0-EFD5114DD5E0"


class Test_CachedDownloadStrategy(unittest.TestCase):
    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.timeseries_api = TimeSeriesAPI(session=self.auth)
        self._cache_root = "./_cache/test_filecachedownload"

        # Set up some stuff here
        self.files_api = FilesAPI(session=self.auth)

        uploader = DirectUpload()

        df = pd.DataFrame({"values": np.arange(100.0)}, index=np.arange(0, 100))

        upload_params = self.files_api.upload()
        self.token_fileid = upload_params["FileId"]

        uploader.put(upload_params, df)

        self.files_api.commit(upload_params["FileId"])






    def tearDown(self):
        self.auth.close()

    def test_get_with_parquet_format(self):
        strategy = BaseDownloader(
            FileCacheDownload(
                cache_root=self._cache_root, format_="parquet"
            )
        )

        response = self.timeseries_api.create()
        myfileid = self.token_fileid

        
        response = self.timeseries_api.add(response["TimeSeriesId"], myfileid)


        chunks = self.timeseries_api.download_days(
            response["TimeSeriesId"], 0,100
        )


        iterations = 100

        usedtime = timeit(stmt=partial(strategy.get, chunks), number=iterations)

        print("Average cache read with parquet: {}".format(usedtime / iterations))

    def test_get_with_csv_format(self):
        strategy = BaseDownloader(
            FileCacheDownload(
                cache_root=self._cache_root, format_="csv"
            )
        )

        response = self.timeseries_api.create()
        myfileid = self.token_fileid

        
        response = self.timeseries_api.add(response["TimeSeriesId"], myfileid)


        chunks = self.timeseries_api.download_days(
            response["TimeSeriesId"], 0,100
        )
        iterations = 100

        usedtime = timeit(stmt=lambda: strategy.get(chunks), number=iterations)

        print("Average cache read with csv: {}".format(usedtime / iterations))


if __name__ == "__main__":
    unittest.main()