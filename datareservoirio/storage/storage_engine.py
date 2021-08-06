import pandas as pd

from azure.storage.blob import BlobClient
from io import BytesIO, TextIOWrapper


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

        with BytesIO() as binary_content:
            self.download_blob().readinto(binary_content)

            binary_content.seek(0)
            df = pd.read_csv(
                TextIOWrapper(binary_content, encoding="utf-8"),
                sep=",",
                index_col=0,
                header=None,
                names=(None, "values"),
                parse_dates=True,
                dtype={"values": "string"},
            )

        df.index = df.index.view("int64")
        try:
            # df = df.astype({"values": "float64"})
            df = df.astype("float64")
        except ValueError:  # unable to cast to float
            pass

        return df

    def create_blob_from_series(self, series):

        if pd.api.types.is_datetime64_ns_dtype(series.index):
            series = series.copy()
            series.index = series.index.view("int64")

        block_data = series.to_csv(header=False, line_terminator="\n")

        self.upload_blob(block_data.encode("ascii"), blob_type="BlockBlob")


class StorageBackend:
    def __init__(self, session=None):
        self._service = AzureBlobService

    def remote_get(self, params):
        return self._service(params).get_blob_to_df()

    def remote_put(self, params, data):
        return self._service(params).create_blob_from_series(data)
