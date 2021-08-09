from io import BytesIO, TextIOWrapper
import timeit
import logging

import numpy as np
import pandas as pd
from azure.storage.blob import BlobClient

log = logging.getLogger(__name__)


class AzureBlobService(BlobClient):
    def __init__(self, params):
        self._account = params["Account"]
        self._sas_key = params["SasKey"]
        self._container_name = params["Container"]
        self._blob_name = params["Path"]
        self._account_url = f"https://{self._account}.blob.core.windows.net"

        super(AzureBlobService, self).__init__(
            self._account_url,
            self._container_name,
            self._blob_name,
            credential=self._sas_key,
        )

    def get_blob_to_df(self):
        time_start = timeit.default_timer()

        with BytesIO() as binary_content:
            log.debug(f"Get chunk {self._blob_name}")
            self.download_blob().readinto(binary_content)

            binary_content.seek(0)
            values_raw = np.asarray(
                list(
                    map(
                        self._split_value,
                        TextIOWrapper(binary_content, encoding="utf-8"),
                    )
                ),
                dtype="O",
            )

            index = values_raw.T[0].astype("int64")
            values = values_raw.T[1]
            try:
                dtype = "float64"
                values = values.astype(dtype)
            except ValueError:  # unable to cast to float
                dtype = "string"
            finally:
                df = pd.DataFrame(values, index, columns=["values"], dtype=dtype)

        time_end = timeit.default_timer()
        log.debug(f"{self._blob_name}: download took {time_end - time_start} seconds")
        return df

    def create_blob_from_series(self, series):

        log.debug(f"upload blob {self._blob_name}")
        if pd.api.types.is_datetime64_ns_dtype(series.index):
            series = series.copy()
            series.index = series.index.view("int64")

        block_data = series.to_csv(header=False, line_terminator="\n")

        self.upload_blob(block_data.encode("ascii"), blob_type="BlockBlob")

    def _split_value(self, line):
        val0, val1 = line.rstrip().split(",", 1)
        if not val1:
            val1 = None
        return val0, val1


class StorageBackend:
    def __init__(self):
        self._service = AzureBlobService

    def remote_get(self, params):
        return self._service(params).get_blob_to_df()

    def remote_put(self, params, data):
        return self._service(params).create_blob_from_series(data)
