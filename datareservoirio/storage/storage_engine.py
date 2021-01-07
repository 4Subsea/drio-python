import base64
import logging
import timeit
from functools import partial
from io import BytesIO, StringIO, TextIOWrapper
from time import sleep

import numpy as np
import pandas as pd
from azure.common import AzureException
from azure.storage.blob import BlobBlock, BlockBlobService

log = logging.getLogger(__name__)


class AzureBlobService(BlockBlobService):
    """
    Sub-class of BlockBlobService that handle upload/download of Pandas Series
    to/from Azure Blob Storage.
    """

    MAX_DOWNLOAD_CONCURRENT_BLOCKS = 4  # benchmark shows no difference?
    MAX_CHUNK_GET_SIZE = 8 * 1024 * 1024
    MAX_SINGLE_GET_SIZE = MAX_CHUNK_GET_SIZE

    def __init__(self, params, session=None):
        """
        Initiate transfer service to Azure Blob Storage.

        Parameters
        ----------
        :param: dict params
            Dict must include:

                * 'Account'
                * 'SasKey'
                * 'Container' (container_name)
                * 'Path' (blob_name)
        :param: requests.Session session
            If specified, passed to the underlying BlockBlobService so that an existing
            request session can be reused.
        """

        self._account = params["Account"]
        self._sas_key = params["SasKey"]
        self.container_name = params["Container"]
        self.blob_name = params["Path"]

        super(AzureBlobService, self).__init__(
            self._account, sas_token=self._sas_key, request_session=session
        )

    def get_blob_to_df(self):
        """
        Download content of the current blob to DataFrame

        Initial parse to frame with string values
        In case values are non-numeric (and possibly ,-separated text),
        we will merge these back into one column of text.
        Otherwise, convert values to float64
        """
        time_start = timeit.default_timer()

        with BytesIO() as binary_content:
            log.debug(f"Get chunk {self.blob_name}")

            self.get_blob_to_stream(
                self.container_name,
                self.blob_name,
                stream=binary_content,
                max_connections=self.MAX_DOWNLOAD_CONCURRENT_BLOCKS,
                progress_callback=lambda current, total: log.info(
                    f"{self.blob_name}: {(current / total) * 100:.1f}% downloaded ({current / (1024 * 1024):.1f} of {total / (1024 * 1024):.1f} MB)"
                ),
            )

            binary_content.seek(0)
            with TextIOWrapper(binary_content, encoding="utf-8") as text_content:
                values_raw = np.asarray(
                    list(map(self._split_value, text_content)), dtype="O"
                )

        index = values_raw.T[0].astype("int64")
        values = values_raw.T[1]
        try:
            dtype = "float64"
            values = values.astype(dtype)
        except ValueError:  # unable to cast to float
            # awaiting Pandas 1.0 string type + pd.NA
            dtype = "O"
        finally:
            df = pd.DataFrame(values, index, columns=["values"], dtype=dtype)

        time_end = timeit.default_timer()
        log.debug(f"{self.blob_name}: download took {time_end - time_start} seconds")
        return df

    def create_blob_from_series(self, series):
        """
        Upload Pandas Series objects to DataReservoir.

        Read only the first column from the Series!

        Parameters
        ----------
        series : pandas.Series
            Appropriately indexed Series
        """
        blocks = []
        leftover = ""
        block_id = 0

        for i, chunk in enumerate(self._gen_line_chunks(series, int(1e6))):
            buf = StringIO()

            if pd.api.types.is_datetime64_ns_dtype(chunk.index):
                chunk = chunk.copy()
                chunk.index = chunk.index.astype("int64")

            chunk.to_csv(buf, header=False, line_terminator="\n")
            buf.seek(0)

            n_blocks = 0
            while True:
                block_data = leftover + buf.read(self.MAX_BLOCK_SIZE - len(leftover))
                leftover = ""
                n_blocks += 1

                if len(block_data) < self.MAX_BLOCK_SIZE:
                    leftover = block_data
                    break

                block = self._make_block(block_id)
                blocks.append(block)
                block_id += 1

                log.debug(f"put block {block.id} for blob {self.blob_name}")
                self.put_block_retry(
                    self.container_name,
                    self.blob_name,
                    block_data.encode("ascii"),
                    block.id,
                )

        if leftover:
            block = self._make_block(block_id)
            blocks.append(block)

            log.debug(f"put block {block.id} for blob {self.blob_name}")
            self.put_block_retry(
                self.container_name,
                self.blob_name,
                block_data.encode("ascii"),
                block.id,
            )

        self.put_block_list(self.container_name, self.blob_name, blocks)

    def put_block_retry(self, *args, **kwargs):
        """put_block with some retry - hotfix"""
        count = 0
        while count <= 5:
            try:
                self.put_block(*args, **kwargs)
                return None
            except AzureException as ex:
                count += 1
                if count > 5:
                    raise ex
                log.debug("put_block_retry retry after AzureException")
                sleep(1 * count)

    def _make_block(self, block_id):
        base64_block_id = self._b64encode(block_id)
        log.debug(f"block id {block_id} blockidbase64 {base64_block_id}")
        return BlobBlock(id=base64_block_id)

    def _b64encode(self, i, length=8):
        i_str = "{0:0{length}d}".format(i, length=length)
        b_ascii = i_str.encode("ascii")
        b_b64 = base64.b64encode(b_ascii)
        return b_b64.decode("ascii")

    def _gen_line_chunks(self, series, n):
        a = 0
        b = n

        while a < len(series):
            yield series.iloc[a:b]
            a += n
            b += n

    def _split_value(self, line):
        val0, val1 = line.rstrip().split(",", 1)
        if not val1:
            val1 = None
        return val0, val1


class StorageBackend:
    def __init__(self, session=None):
        self._service = partial(AzureBlobService, session=session)

    def remote_get(self, params):
        return self._service(params).get_blob_to_df()

    def remote_put(self, params, data):
        return self._service(params).create_blob_from_series(data)
