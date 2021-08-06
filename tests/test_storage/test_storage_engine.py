import pandas as pd
import pytest
from unittest.mock import Mock, patch

from datareservoirio.storage.storage_engine import StorageBackend, AzureBlobService


@pytest.fixture
def blob_params():
    params = {
        "Account": "some_account",
        "SasKey": "some_sas_key",
        "Container": "some_container",
        "Path": "some_path",
    }
    return params


class Test_AzureBlobService:

    @patch("datareservoirio.storage.storage_engine.BlobClient.__init__")
    def test__init__(self, mock_blob_client__init__, blob_params):

        blob_client = AzureBlobService(blob_params)

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

        with patch.object(AzureBlobService, "download_blob", return_value=mock_download):
            blob_client = AzureBlobService(blob_params)

            df_out = blob_client.get_blob_to_df()
            idx_expect = [1609459200000000000, 1609459200100000000, 1609459200200000000]
            vals_expect = [0.0, 0.1, 1.13]
            df_expect = pd.DataFrame(
                index=pd.Int64Index(idx_expect),
                data={"values": vals_expect},
                dtype="float64"
            )
            df_expect.index = df_expect.index.view("int64")

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_blob_to_df_w_empty(self, blob_params):

        mock_download = Mock()
        binary_content = (
            "1,0.0\r\n"
            + "2,\r\n"
            + "3,1.13"
        ).encode("utf-8")
        mock_download.readinto.side_effect = lambda binary_stream: binary_stream.write(
            binary_content
        )

        with patch.object(AzureBlobService, "download_blob", return_value=mock_download):
            blob_client = AzureBlobService(blob_params)

            df_out = blob_client.get_blob_to_df()
            df_expect = pd.DataFrame(
                index=pd.Int64Index([1, 2, 3]),
                data={"values": [0.0, None, 1.13]},
                dtype="float64"
            )

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

        with patch.object(AzureBlobService, "download_blob", return_value=mock_download):
            blob_client = AzureBlobService(blob_params)

            df_out = blob_client.get_blob_to_df()
            idx_expect = [1609459200000000000, 1609459200100000000, 1609459200200000000]
            vals_expect = ["some_string", "testing", "hello"]
            df_expect = pd.DataFrame(
                index=pd.Int64Index(idx_expect),
                data={"values": vals_expect},
                dtype="string"
            )

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_blob_to_df_nonnumeric(self, blob_params):

        mock_download = Mock()
        binary_content = (
            "1609459200000000000,some_string\r\n"
            + "1609459200100000000,$GPGGA,,112359.00,6112.852865,N,00045.206912,E,2,07,1.1,60.96,M,47.02,M,7.4,0685*74\r\n"
            + "1609459200200000000,$GPRMC,112440.00,A,6112.852904,N,00045.206762,E,0.0,304.90,221119,0.9,W,D*1F"
        ).encode("utf-8")
        mock_download.readinto.side_effect = lambda binary_stream: binary_stream.write(
            binary_content
        )

        with patch.object(AzureBlobService, "download_blob", return_value=mock_download):
            blob_client = AzureBlobService(blob_params)

            df_out = blob_client.get_blob_to_df()
            idx_expect = [1609459200000000000, 1609459200100000000, 1609459200200000000]
            vals_expect = ["some_string", "$GPGGA,,112359.00,6112.852865,N,00045.206912,E,2,07,1.1,60.96,M,47.02,M,7.4,0685*74", "$GPRMC,112440.00,A,6112.852904,N,00045.206762,E,0.0,304.90,221119,0.9,W,D*1F"]
            df_expect = pd.DataFrame(
                index=pd.Int64Index(idx_expect),
                data={"values": vals_expect},
                dtype="string"
            )

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_get_blob_to_df_string_w_empty(self, blob_params):

        mock_download = Mock()
        binary_content = (
            "1,some_string\r\n"
            + "2,\r\n"
            + "3,hello"
        ).encode("utf-8")
        mock_download.readinto.side_effect = lambda binary_stream: binary_stream.write(
            binary_content
        )

        with patch.object(AzureBlobService, "download_blob", return_value=mock_download):
            blob_client = AzureBlobService(blob_params)

            df_out = blob_client.get_blob_to_df()
            df_expect = pd.DataFrame(
                index=pd.Int64Index([1, 2, 3]),
                data={"values": ["some_string", None, "hello"]},
                dtype="string"
            )

            pd.testing.assert_frame_equal(df_out, df_expect)

    def test_create_blob_from_series(self, blob_params):
        series_vals = [1.1, 2.3, 0.2]
        series_idx = [1609459200000000000, 1609459200100000000, 1609459200200000000]
        series = pd.Series(
            data=series_vals,
            index=series_idx
        )

        with patch.object(AzureBlobService, "upload_blob") as mock_upload:
            blob_client = AzureBlobService(blob_params)

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

        with patch.object(AzureBlobService, "upload_blob") as mock_upload:
            blob_client = AzureBlobService(blob_params)

            blob_client.create_blob_from_series(series)

            series_csv = (
                "1609459200000000000,1.1\n"
                + "1609459200100000000,2.3\n"
                + "1609459200200000000,0.2\n"
            )
            mock_upload.assure_called_once_with(
                series_csv.encode("ascii"), blob_type="BlockBlob"
            )

    def test_split_value(self, blob_params):
        uploader = AzureBlobService(blob_params)
        value = "1,2"
        output = uploader._split_value(value)
        expected = ("1", "2")
        assert output == expected

    def test_split_value_nan(self, blob_params):
        uploader = AzureBlobService(blob_params)
        value = "1,"
        output = uploader._split_value(value)
        expected = ("1", None)
        assert output == expected


class TestStorageBackend:
    def test__init__(self, blob_params):
        storage = StorageBackend()
        assert isinstance(storage._service(blob_params), AzureBlobService)

    def test_remote_get(self, blob_params):
        backend = StorageBackend()
        with patch.object(backend, "_service") as mock_service:
            backend.remote_get(blob_params)
            mock_service.assert_called_once_with(blob_params)
            mock_service.return_value.get_blob_to_df.assert_called_once()

    def test_remote_put(self, blob_params):
        backend = StorageBackend()
        with patch.object(backend, "_service") as mock_service:
            backend.remote_put(blob_params, "data")
            mock_service.assert_called_once_with(blob_params)
            mock_service.return_value.create_blob_from_series.assert_called_once_with("data")
