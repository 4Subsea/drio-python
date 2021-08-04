import numpy as np
import pandas as pd

from azure.storage.blob import BlobClient
from io import BytesIO, TextIOWrapper, StringIO


# class _AzureBlobClient(BlobClient):
#     def __init__(self, params):
#         self._account = params["Account"]
#         self._sas_key = params["SasKey"]
#         self._container_name = params["Container"]
#         self._blob_name = params["Path"]
#         self._account_url = f"https://{self._account}.blob.core.windows.net"

#         super(AzureBlobClient, self).__init__(
#             self._account_url, self._container_name, self._blob_name, credential=self._sas_key
#         )

#     def get_blob_to_df(self):
#         with BytesIO() as binary_content:
#             self.download_blob().readinto(binary_content)

#             binary_content.seek(0)
#             with TextIOWrapper(binary_content, encoding="utf-8") as text_content:
#                 values_raw = np.asarray(
#                     list(map(self._split_value, text_content)), dtype="O"
#                 )

#         index = values_raw.T[0].astype("int64")
#         values = values_raw.T[1]
#         try:
#             dtype = "float64"
#             values = values.astype(dtype)
#         except ValueError:  # unable to cast to float
#             # awaiting Pandas 1.0 string type + pd.NA
#             dtype = "O"
#         finally:
#             df = pd.DataFrame(values, index, columns=["values"], dtype=dtype)

#         return df

#     def _split_value(self, line):
#         val0, val1 = line.rstrip().split(",", 1)
#         if not val1:
#             val1 = None
#         return val0, val1

#     def create_blob_from_series(self, series):

#         data = series.copy()
#         data.index = data.index.view("int64")

#         block_data = data.to_csv(header=False, line_terminator="\n")

#         self.upload_blob(block_data.encode("ascii"), blob_type="BlockBlob")


# class AzureBlobClient(BlobClient):
#     def __init__(self, params):
#         self._account = params["Account"]
#         self._sas_key = params["SasKey"]
#         self._container_name = params["Container"]
#         self._blob_name = params["Path"]
#         self._account_url = f"https://{self._account}.blob.core.windows.net"

#         super(AzureBlobClient, self).__init__(
#             self._account_url,
#             self._container_name,
#             self._blob_name,
#             credential=self._sas_key,
#         )

#     def get_blob_to_df(self):
#         text_content = self.download_blob().readall().decode(encoding="utf-8")

#         df = pd.read_csv(
#             StringIO(text_content),
#             sep=",",
#             index_col=0,
#             header=None,
#             names=(None, "values"),
#             dtype="O",
#         )

#         df.index = df.index.view("int64")
#         try:
#             df = df.astype({"values": "float64"})
#         except ValueError:   # unable to cast to float
#             pass

#         return df

#     def create_blob_from_series(self, series):

#         if pd.api.types.is_datetime64_ns_dtype(series.index):
#             series = series.copy()
#             series.index = series.index.astype("int64")

#         block_data = series.to_csv(header=False, line_terminator="\n")

#         self.upload_blob(block_data.encode("ascii"), blob_type="BlockBlob")


class AzureBlobClient(BlobClient):
    def __init__(self, params):
        self._account = params["Account"]
        self._sas_key = params["SasKey"]
        self._container_name = params["Container"]
        self._blob_name = params["Path"]
        self._account_url = f"https://{self._account}.blob.core.windows.net"

        super(AzureBlobClient, self).__init__(
            self._account_url,
            self._container_name,
            self._blob_name,
            credential=self._sas_key,
        )

    def get_blob_to_df(self):

        with BytesIO() as binary_content:
            self.download_blob().readinto(binary_content)

            binary_content.seek(0)
            with TextIOWrapper(binary_content, encoding="utf-8") as text_content:
                df = pd.read_csv(
                    text_content,
                    sep=",",
                    index_col=0,
                    header=None,
                    names=(None, "values"),
                    dtype="O",
                )

        df.index = df.index.view("int64")
        try:
            df = df.astype({"values": "float64"})
        except ValueError:  # unable to cast to float
            pass

        return df

    def create_blob_from_series(self, series):

        if pd.api.types.is_datetime64_ns_dtype(series.index):
            series = series.copy()
            series.index = series.index.astype("int64")

        block_data = series.to_csv(header=False, line_terminator="\n")

        self.upload_blob(block_data.encode("ascii"), blob_type="BlockBlob")


class StorageBackend:
    def __init__(self, session=None):
        self._service = AzureBlobClient

    def remote_get(self, params):
        return self._service(params).get_blob_to_df()

    def remote_put(self, params, data):
        return self._service(params).create_blob_from_series(data)
