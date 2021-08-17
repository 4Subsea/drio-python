import logging
import timeit
from io import BytesIO, TextIOWrapper

import numpy as np
import pandas as pd
from azure.storage.blob import BlobClient

log = logging.getLogger(__name__)


class AzureBlobService(BlobClient):
    """
    Sub-class of BlobClient that handle upload/download of Pandas Series to/from
    Azure Blob Storage.
    """

    def __init__(self, params):
        """
        Initiate transfer service to Azure Blob Storage.

        Parameters
        ----------
        params : dict
            Dict must include:

                * 'Account'
                * 'SasKey'
                * 'Container' (container_name)
                * 'Path' (blob_name)
        """

        self._account = params["Account"]
        self._sas_key = params["SasKey"]
        self._container_name = params["Container"]
        self._blob_name = params["Path"]
        self._version_id = params.get("VersionId", None)
        self._account_url = f"https://{self._account}.blob.core.windows.net"

        super(AzureBlobService, self).__init__(
            self._account_url,
            self._container_name,
            self._blob_name,
            credential=self._sas_key,
        )

    def get_blob_to_df(self):
        """
        Download content of the current blob to Pandas DataFrame.

        Initial parse to frame with string values. In case values are non-numeric
        (and possibly ,-separated text), we will merge these back into one column
        of text. Otherwise, convert values to float64.
        """

        time_start = timeit.default_timer()

        with BytesIO() as binary_content:
            log.debug(f"Get chunk {self._blob_name}")
            self.download_blob(version_id=self._version_id).readinto(binary_content)

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
        """
        Upload Pandas series object to DataReservoir.

        Read only the first column of the series!

        Parameters
        ----------
        series : pandas.Series
            Approprately indexed Series.
        """

        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]

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
    """
    Handles upload/download of Pandas Series to/from Azure Blob Storage.
    """

    def __init__(self):
        self._service = AzureBlobService

    def remote_get(self, params):
        """Get data."""
        return self._service(params).get_blob_to_df()

    def remote_put(self, params, data):
        """Upload data."""
        return self._service(params).create_blob_from_series(data)
