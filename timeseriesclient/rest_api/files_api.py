from __future__ import absolute_import

import base64
import logging
import sys
import timeit
from functools import wraps

if sys.version_info.major == 3:
    from io import StringIO
elif sys.version_info.major == 2:
    from cStringIO import StringIO

from azure.storage.blob import BlobBlock, BlockBlobService

from .base_api import BaseApi, TokenAuth
from ..log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class FilesApi(BaseApi):

    def __init__(self):
        super(FilesApi, self).__init__()

    def upload(self, token):
        logwriter.debug("called", "upload")

        uri = self._api_base_url + 'Files/upload'
        response = self._post(uri, auth=TokenAuth(token))
        return response.json()

    @staticmethod
    def upload_service(upload_params):
        logwriter.debug("called", "upload_service")
        return AzureBlobService(upload_params)

    def commit(self, token, file_id):
        logwriter.debug("called", "commit")

        uri = self._api_base_url + 'Files/commit'
        body = { 'FileId' : file_id }
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.status_code

    def status(self, token, file_id):
        logwriter.debug("called", "status")

        uri = self._api_base_url + 'files/{}/status'.format(file_id)
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def ping(self, token):
        logwriter.debug("called", "ping")

        uri = self._api_base_url + 'ping'
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()


class AzureBlobService(BlockBlobService):

    def __init__(self, upload_params):
        self.container_name = upload_params['Container']
        self.blob_name = upload_params['Path']
        self.file_id = upload_params['FileId']

        super(AzureBlobService, self).__init__(upload_params['Account'],
                                                sas_token=upload_params['SasKey'])

    def create_blob_from_df(self, dataframe):
        blocks = []
        leftover = ''
        block_id = 0

        for i, chunk in enumerate(self._gen_line_chunks(dataframe, int(1e6))):
            buf = StringIO()

            if chunk.index.dtype == 'datetime64[ns]':
                chunk = chunk.copy()
                chunk.index = chunk.index.astype('int64')

            chunk.to_csv(buf, header=False)
            buf.seek(0)

            n_blocks = 0
            while True:
                block_data = leftover + buf.read(self.MAX_BLOCK_SIZE -
                                                 len(leftover))
                leftover = ''
                n_blocks += 1

                if len(block_data) < self.MAX_BLOCK_SIZE:
                    leftover = block_data
                    break

                block = self._make_block(block_id)
                blocks.append(block)
                block_id += 1

                logwriter.debug("put block {} for blob {}".format(block.id, self.blob_name), 'put_block')
                self.put_block(self.container_name, self.blob_name,
                               block_data.encode('ascii'), block.id)

        if leftover:
            block = self._make_block(block_id)
            blocks.append(block)

            logwriter.debug("put block {} for blob {}".format(block.id, self.blob_name), 'put_block')
            self.put_block(self.container_name, self.blob_name,
                           block_data.encode('ascii'), block.id)

        self.put_block_list(self.container_name, self.blob_name, blocks)

    def _make_block(self, block_id):
        base64_block_id = self._b64encode(block_id)
        logwriter.debug("block id {} blockidbase64 {}".format(block_id,
                                                              base64_block_id), '_make_block')
        return BlobBlock(id=base64_block_id)

    def _b64encode(self, i, length=8):
        i_str = '{0:0{length}d}'.format(i, length=length)
        b_ascii= i_str.encode('ascii')
        b_b64  = base64.b64encode(b_ascii)
        return b_b64.decode('ascii')

    def _gen_line_chunks(self, dataframe, n):
        a = 0
        b = n

        while a<len(dataframe):
            yield dataframe.iloc[a:b]
            a += n
            b += n
