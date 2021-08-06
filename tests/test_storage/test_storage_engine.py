import pandas as pd
import numpy as np
import pytest
from unittest.mock import Mock, patch

import datareservoirio as drio
from datareservoirio.storage.storage_engine import StorageBackend, AzureBlobClient


# @pytest.fixture
# def blob_client_mock():
#     blob_client = Mock()

#     def readinto_side_effect(binary_stream):
#         binary_content = "1609459200000000000,0.0\r\n1609459200100000000,7.272205216578941e-06\r\n1609459200200000000,1.4544410432773292e-05".encode("utf-8")
#         binary_stream.write(binary_content)

#     blob_client.download_blob.return_value.readinto.side_effect = readinto_side_effect

#     return blob_client


# blob_client_mock = Mock()
# binary_content = binary_content = "1609459200000000000,0.0\r\n1609459200100000000,7.272205216578941e-06\r\n1609459200200000000,1.4544410432773292e-05".encode("utf-8")
# blob_client_mock.download_blob.return_value.readinto.side_effect = lambda binary_stream: binary_stream.write(binary_content)


@pytest.fixture
def blob_params():
    params = {
        "Account": "some_account",
        "SasKey": "some_sas_key",
        "Container": "some_container",
        "Path": "some_path",
    }
    return params


class Test_AzureBlobClient:

    @patch("datareservoirio.storage.storage_engine.BlobClient.__init__")
    def test__init__(self, mock_blob_client__init__, blob_params):

        blob_client = AzureBlobClient(blob_params)

        assert blob_client._account == "some_account"
        assert blob_client._sas_key == "some_sas_key"
        assert blob_client._container_name == "some_container"
        assert blob_client._blob_name == "some_path"
        assert blob_client._account_url == "https://some_account.blob.core.windows.net"

        mock_blob_client__init__.assert_called_once_with(
            "https://some_account.blob.core.windows.net",
            "some_container",
            "some_path",
            credential="some_sas_key",
        )

    def test_get_blob_to_df(self, blob_params):

        mock_download = Mock()
        binary_content = (
            "1609459200000000000,0.0\r\n"
            + "1609459200100000000,0.1\r\n"
            + "1609459200200000000,1.13"
        ).encode("utf-8")
        mock_download.readinto.side_effect = lambda binary_stream: binary_stream.write(
            binary_content
        )

        with patch.object(AzureBlobClient, "download_blob", return_value=mock_download):
            blob_client = AzureBlobClient(blob_params)

            df_out = blob_client.get_blob_to_df()
            idx_expect = [1609459200000000000, 1609459200100000000, 1609459200200000000]
            vals_expect = [0.0, 0.1, 1.13]
            df_expect = pd.DataFrame(
                index=idx_expect,
                data={"values": vals_expect},
                dtype="float64"
            )
            df_expect.index = df_expect.index.view("int64")

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_blob_to_df_datetime64(self, blob_params):

        mock_download = Mock()
        binary_content = (
            "2021-01-01 00:00:00,0.0\r\n"
            + "2021-01-01 00:00:00.100000,0.1\r\n"
            + "2021-01-01 00:00:00.200000,1.13"
        ).encode("utf-8")
        mock_download.readinto.side_effect = lambda binary_stream: binary_stream.write(
            binary_content
        )

        with patch.object(AzureBlobClient, "download_blob", return_value=mock_download):
            blob_client = AzureBlobClient(blob_params)

            df_out = blob_client.get_blob_to_df()
            idx_expect = [1609459200000000000, 1609459200100000000, 1609459200200000000]
            vals_expect = [0.0, 0.1, 1.13]
            df_expect = pd.DataFrame(
                index=idx_expect,
                data={"values": vals_expect},
                dtype="float64"
            )
            df_expect.index = df_expect.index.view("int64")

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_blob_to_df_string(self, blob_params):

        mock_download = Mock()
        binary_content = (
            "1609459200000000000,some_string\r\n"
            + "1609459200100000000,testing\r\n"
            + "1609459200200000000,hello"
        ).encode("utf-8")
        mock_download.readinto.side_effect = lambda binary_stream: binary_stream.write(
            binary_content
        )

        with patch.object(AzureBlobClient, "download_blob", return_value=mock_download):
            blob_client = AzureBlobClient(blob_params)

            df_out = blob_client.get_blob_to_df()
            idx_expect = [1609459200000000000, 1609459200100000000, 1609459200200000000]
            vals_expect = ["some_string", "testing", "hello"]
            df_expect = pd.DataFrame(
                index=idx_expect,
                data={"values": vals_expect},
                dtype="string"
            )
            df_expect.index = df_expect.index.view("int64")

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_create_blob_from_series(self, blob_params):
        series_vals = [1.1, 2.3, 0.2]
        series_idx = [1609459200000000000, 1609459200100000000, 1609459200200000000]
        series = pd.Series(
            data=series_vals,
            index=series_idx
        )

        with patch.object(AzureBlobClient, "upload_blob") as mock_upload:
            blob_client = AzureBlobClient(blob_params)

            blob_client.create_blob_from_series(series)

            series_csv = (
                "1609459200000000000,1.1\n"
                + "1609459200100000000,2.3\n"
                + "1609459200200000000,0.2\n"
            )
            mock_upload.assure_called_once_with(
                series_csv.encode("ascii"), blob_type="BlockBlob"
            )

    def test_create_blob_from_series_datetime64(self, blob_params):
        series_vals = [1.1, 2.3, 0.2]
        series_idx = [1609459200000000000, 1609459200100000000, 1609459200200000000]
        series = pd.Series(
            data=series_vals,
            index=pd.to_datetime(series_idx)
        )

        with patch.object(AzureBlobClient, "upload_blob") as mock_upload:
            blob_client = AzureBlobClient(blob_params)

            blob_client.create_blob_from_series(series)

            series_csv = (
                "1609459200000000000,1.1\n"
                + "1609459200100000000,2.3\n"
                + "1609459200200000000,0.2\n"
            )
            mock_upload.assure_called_once_with(
                series_csv.encode("ascii"), blob_type="BlockBlob"
            )


class TestStorageBackend:
    def test__init__(self, blob_params):
        storage = StorageBackend()
        assert isinstance(storage._service(blob_params), AzureBlobClient)

    def test_remote_get(self, blob_params):
        with patch("datareservoirio.storage.storage_engine.AzureBlobClient") as mock_service:
            storage = StorageBackend()
            storage.remote_get(blob_params)
            mock_service.assert_called_once_with(blob_params)
            mock_service.return_value.get_blob_to_df.assert_called_once()

    def test_remote_put(self, blob_params):
        with patch("datareservoirio.storage.storage_engine.AzureBlobClient") as mock_service:
            storage = StorageBackend()
            storage.remote_put(blob_params, "test")
            mock_service.assert_called_once_with(blob_params)
            mock_service.return_value.create_blob_from_series.assert_called_once_with("test")
