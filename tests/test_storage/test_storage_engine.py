import io
import unittest
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import requests
from azure.storage.blob import BlobBlock

from datareservoirio.storage.storage_engine import (
    AzureBlobService,
    AzureException,
    StorageBackend,
)


class Test_AzureBlobService(unittest.TestCase):
    def setUp(self):
        self.upload_params = {
            "Account": "account_xyz",
            "Container": "blobcontainer",
            "Path": "blob_xy",
            "FileId": "file_123abc",
            "SasKey": "sassykeiz",
        }

    def test_constructor(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        expected_attributes = ["container_name", "blob_name"]
        for attribute in expected_attributes:
            if not hasattr(uploader, attribute):
                self.fail("Expected uploader to have attribute {}".format(attribute))

    def test_download(self):
        downloader = AzureBlobService(self.upload_params, session=MagicMock())

        with patch.object(downloader, "get_blob_to_stream"):
            with patch(
                "datareservoirio.storage.storage_engine.TextIOWrapper"
            ) as bytesio:
                bytesio.return_value.__enter__.return_value = [
                    "1,1.0\r\n",
                    "2,2.0\r\n",
                    "3,3.0\r\n",
                ]

                df_out = downloader.get_blob_to_df()

        df_expected = pd.DataFrame(
            {"values": [1.0, 2.0, 3.0]}, index=pd.Int64Index([1, 2, 3])
        )

        pd.testing.assert_frame_equal(df_out, df_expected)

    def test_download_numeric_w_empty(self):
        downloader = AzureBlobService(self.upload_params, session=MagicMock())

        with patch.object(downloader, "get_blob_to_stream"):
            with patch(
                "datareservoirio.storage.storage_engine.TextIOWrapper"
            ) as bytesio:
                bytesio.return_value.__enter__.return_value = [
                    "1,1.0\r\n",
                    "2,\r\n",
                    "3,3.0\r\n",
                ]

                df_out = downloader.get_blob_to_df()

        df_expected = pd.DataFrame(
            {"values": [1.0, None, 3.0]}, index=pd.Int64Index([1, 2, 3])
        )

        pd.testing.assert_frame_equal(df_out, df_expected)

    def test_download_nonnumericdata(self):
        downloader = AzureBlobService(self.upload_params, session=MagicMock())

        with patch.object(downloader, "get_blob_to_stream"):
            with patch(
                "datareservoirio.storage.storage_engine.TextIOWrapper"
            ) as bytesio:
                bytesio.return_value.__enter__.return_value = [
                    "1374421840494003000,next value is empty\r\n",
                    "1474421840494003000,\r\n",
                    "1574421840494003000,$GPGGA,,112359.00,6112.852865,N,00045.206912,E,2,07,1.1,60.96,M,47.02,M,7.4,0685*74\r\n",
                    "1674421882488006000,$GPRMC,112440.00,A,6112.852904,N,00045.206762,E,0.0,304.90,221119,0.9,W,D*1F\r\n",
                ]

                df_out = downloader.get_blob_to_df()

        df_expected = pd.DataFrame(
            {
                "values": [
                    "next value is empty",
                    None,
                    "$GPGGA,,112359.00,6112.852865,N,00045.206912,E,2,07,1.1,60.96,M,47.02,M,7.4,0685*74",
                    "$GPRMC,112440.00,A,6112.852904,N,00045.206762,E,0.0,304.90,221119,0.9,W,D*1F",
                ]
            },
            index=pd.Int64Index(
                [
                    1374421840494003000,
                    1474421840494003000,
                    1574421840494003000,
                    1674421882488006000,
                ]
            ),
        )

        pd.testing.assert_frame_equal(df_out, df_expected)

    def test_upload(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        uploader.put_block = Mock()
        uploader.put_block_list = Mock()

        series = pd.Series(np.arange(500))
        uploader.create_blob_from_series(series)

        uploader.put_block.assert_called_once_with(
            self.upload_params["Container"],
            self.upload_params["Path"],
            series.to_csv(header=False, line_terminator="\n").encode("ascii"),
            "MDAwMDAwMDA=",
        )
        uploader.put_block_list.assert_called_once()

    def test_upload_converts_datetime64_to_int64(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        uploader.put_block = Mock()
        uploader.put_block_list = Mock()

        timevector = np.array(np.arange(0, 1001e9, 1e9), dtype="datetime64[ns]")
        series = pd.Series(np.arange(1001), index=timevector)
        uploader.create_blob_from_series(series)

        series.index = series.index.astype(np.int64)
        uploader.put_block.assert_called_once_with(
            self.upload_params["Container"],
            self.upload_params["Path"],
            series.to_csv(header=False, line_terminator="\n").encode("ascii"),
            "MDAwMDAwMDA=",
        )
        uploader.put_block_list.assert_called_once()

    def test_upload_long(self):
        side_effect = 4 * [None]

        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        series = pd.Series(np.arange(1.001e6))
        uploader.create_blob_from_series(series)

        # 4 was just found empirically
        self.assertEqual(uploader.put_block.call_count, 4)

    @patch("datareservoirio.storage.storage_engine.sleep")
    def test_upload_long_w_azureexception(self, mock_sleep):
        side_effect = 3 * [AzureException] + 4 * [None]

        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        series = pd.Series(np.arange(1.001e6))
        uploader.create_blob_from_series(series)

        # 4 was just found empirically + 3 error
        self.assertEqual(uploader.put_block.call_count, 4 + 3)

    @patch("datareservoirio.storage.storage_engine.sleep")
    def test_upload_raise_azureexception(self, mock_sleep):
        side_effect = 6 * [AzureException] + 4 * [None]

        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        uploader.put_block = Mock(side_effect=side_effect)
        uploader.put_block_list = Mock()

        series = pd.Series(np.arange(1.001e6))

        with self.assertRaises(AzureException):
            uploader.create_blob_from_series(series)

    def test_make_block(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        block = uploader._make_block(0)

        self.assertIsInstance(block, BlobBlock)
        self.assertEqual(block.id, "MDAwMDAwMDA=")

    def test_b64encode(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        b64_result = uploader._b64encode(0)
        b64_expected = "MDAwMDAwMDA="
        self.assertEqual(b64_result, b64_expected)

        b64_result = uploader._b64encode(1)
        b64_expected = "MDAwMDAwMDE="
        self.assertEqual(b64_result, b64_expected)

        b64_result = uploader._b64encode(int(1e6))
        b64_expected = "MDEwMDAwMDA="
        self.assertEqual(b64_result, b64_expected)

    def test_gen_line_chunks(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        df = pd.DataFrame(np.arange(999))

        for i, chunk in enumerate(uploader._gen_line_chunks(df, 100)):
            if i < 9:
                self.assertEqual(len(chunk), 100)
            elif i == 9:
                self.assertEqual(len(chunk), 99)
            else:
                self.fail("Too many iterations")

        self.assertEqual(i + 1, 10)

    def test_split_value(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        value = "1,2"
        output = uploader._split_value(value)
        expected = ("1", "2")
        self.assertTupleEqual(output, expected)

    def test_split_value_nan(self):
        uploader = AzureBlobService(self.upload_params, session=MagicMock())
        value = "1,"
        output = uploader._split_value(value)
        expected = ("1", None)
        self.assertTupleEqual(output, expected)


class Test_StorageBackend(unittest.TestCase):
    def setUp(self):
        self._dummy_params = {
            "Account": "some_account",
            "SasKey": "some_key",
            "Container": "yes_please",
            "Path": "looking_for_it",
        }

    def test__init(self):
        with patch("datareservoirio.storage.storage_engine.partial") as mock_partial:
            StorageBackend(session="hello")
        mock_partial.assert_called_once_with(AzureBlobService, session="hello")

    def test__service_is_azure_blob(self):
        backend = StorageBackend()
        self.assertIsInstance(backend._service(self._dummy_params), AzureBlobService)

    def test_get(self):
        backend = StorageBackend()

        with patch.object(backend, "_service") as mock_service:
            mock_obj = MagicMock()
            mock_service.return_value = mock_obj

            backend.remote_get(self._dummy_params)
            mock_service.assert_called_once_with(self._dummy_params)
            mock_obj.get_blob_to_df.assert_called_once()

    def test_put(self):
        backend = StorageBackend()

        with patch.object(backend, "_service") as mock_service:
            mock_obj = MagicMock()
            mock_service.return_value = mock_obj

            backend.remote_put(self._dummy_params, "data")
            mock_service.assert_called_once_with(self._dummy_params)
            mock_obj.create_blob_from_series.assert_called_once_with("data")


if __name__ == "__main__":
    unittest.main()
