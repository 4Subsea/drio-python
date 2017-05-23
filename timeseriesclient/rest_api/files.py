from __future__ import absolute_import

import base64
import logging
import sys
import timeit
from functools import wraps
from time import sleep

from azure.storage.blob import BlobBlock, BlockBlobService
from azure.storage.storageclient import AzureException

from ..log import LogWriter
from .base import BaseAPI, TokenAuth

if sys.version_info.major == 3:
    from io import StringIO
elif sys.version_info.major == 2:
    from cStringIO import StringIO


logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class FilesAPI(BaseAPI):
    """
    Python wrapper for reservoir-api.4subsea.net/api/files
    """

    def __init__(self):
        super(FilesAPI, self).__init__()

    def upload(self, token):
        """
        Create file entry in the reservoir.

        Parameters
        ----------
        token : dict
            authentication token

        Return
        ------
        dict
            Parameters requried by the uploader service
        """

        logwriter.debug("called with <token>", "upload")

        uri = self._api_base_url + 'Files/upload'
        response = self._post(uri, auth=TokenAuth(token))

        for key, value in response.json().iteritems():
            logwriter.debug("parameter received - {key}: {value}"
                            .format(key=key, value=value), "upload")
        return response.json()

    @staticmethod
    def download_service(account_params):
        """
        Initiate a blob service.

        An instance of a subclass of Azure Storage BlockBlobService is
        returned.

        Parameters
        ----------
        account_params : dict
            account/key parameters recieved by e.g. `upload` method

        Return
        ------
        object
            Pre-configured blob service
        """

        return AzureBlobDownloadService(account_params)


    @staticmethod
    def upload_service(upload_params):
        """
        Initiate an uploader service.

        An instance of a subclass of Azure Storage BlockBlobService is
        returned.

        Parameters
        ----------
        upload_params : dict
            parameters recieved by `upload` method

        Return
        ------
        object
            Pre-configured uploader service
        """

        logwriter.debug("called <token>, <upload_params>", "upload_service")
        return AzureBlobService(upload_params)

    def commit(self, token, file_id):
        """
        Commit a file.

        Parameters
        ----------
        token : dict
            authentication token
        file_id : str
            id of file (Files API) to be commit.

        Return
        ------
        str
            HTTP status code
        """
        logwriter.debug("called with <token>, {}".format(file_id), "commit")

        uri = self._api_base_url + 'Files/commit'
        body = {'FileId': file_id}
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.status_code

    def bytes(self, token, file_id):
        """
        Return file as csv.

        Parameters
        ----------
        token : dict
            authentication token
        file_id : str
            id of file (Files API) to be commit.

        Return
        ------
        str
            csv with data
        """
        logwriter.debug("called with <token>, {}".format(file_id), "bytes")

        uri = self._api_base_url + 'files/{}/bytes'.format(file_id)

        response = self._get(uri, auth=TokenAuth(token))
        return response.text

    def status(self, token, file_id):
        """
        Probe file status.

        Parameters
        ----------
        token : dict
            authentication token
        file_id : str
            id of file (Files API) to be commit.

        Return
        ------
        str
            "Unitialized", "Processing", "Ready", or "Failed"
        """
        logwriter.debug("called with <token>, {}".format(file_id), "status")

        uri = self._api_base_url + 'files/{}/status'.format(file_id)
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def ping(self, token):
        """
        Ping server.

        Parameters
        ----------
        token : dict
            authentication token

        Return
        ------
        dict
            pong
        """
        logwriter.debug("called <token>", "ping")

        uri = self._api_base_url + 'ping'
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

class AzureBlobDownloadService(BlockBlobService):
    """Sub-class of BlockBlobService for downloading blobs from Storage
    
    :ivar int MAX_DOWNLOAD_CONCURRENT_BLOCKS: 
        Max connections to use for downloading blob blocks.

    """

    MAX_DOWNLOAD_CONCURRENT_BLOCKS = 32

    def __init__(self, account_params):
        """
        Initiate download service to Azure Blob Storage.

        Parameters
        ----------
        account_params : dict
            Dict must include:

                * "Account"
                * "SasKey"
                * "Container" (container_name)
                * "Path" (blob_name)
        """

        super(AzureBlobDownloadService, self).__init__(
            account_params['Account'], sas_token=account_params['SasKey'])
        self.MAX_CHUNK_GET_SIZE = 8 * 1024 * 1024
        self.MAX_SINGLE_GET_SIZE = self.MAX_CHUNK_GET_SIZE
        self.container_name = account_params['Container']
        self.blob_name = account_params['Path']

    def get_content_to_stream(self, stream, progress_callback=None):
        """
        Download content of the current blob

        Parameters
        ----------
        stream : stream
            A writable stream that will receive the bytes downloaded for the current blob

        """

        time_start = timeit.default_timer()

        blob = self.get_blob_to_stream(
            self.container_name, self.blob_name,
            stream=stream,
            max_connections=self.MAX_DOWNLOAD_CONCURRENT_BLOCKS,
            progress_callback=lambda current,total: progress_callback(current/self.MAX_CHUNK_GET_SIZE, total/self.MAX_CHUNK_GET_SIZE))

        time_end = timeit.default_timer()
        logwriter.debug('Blob download took {} seconds'
                       .format(time_end - time_start), 'get_content')

        return blob


class AzureBlobService(BlockBlobService):
    """Sub-class of BlockBlobService"""

    def __init__(self, upload_params):
        """
        Initiate uploader service to Azure Blob Storage.

        Parameters
        ----------
        upload_params : dict
            Dict must include:

                * "Account"
                * "SasKey"
                * "Container" (container_name)
                * "Path" (blob_name),
                * "FileId" (file_id)
        """

        self.container_name = upload_params['Container']
        self.blob_name = upload_params['Path']
        self.file_id = upload_params['FileId']

        super(AzureBlobService, self).__init__(
            upload_params['Account'], sas_token=upload_params['SasKey'])

    def create_blob_from_df(self, dataframe):
        """
        Upload Pandas DataFrame objects to the reservoir.

        Parameters
        ----------
        dataframe : Pandas DataFrame
            Appropriately indexed single column dataframe
        """
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

                logwriter.debug("put block {} for blob {}".format(
                    block.id, self.blob_name), 'put_block')
                self.put_block_retry(self.container_name, self.blob_name,
                                     block_data.encode('ascii'), block.id)

        if leftover:
            block = self._make_block(block_id)
            blocks.append(block)

            logwriter.debug("put block {} for blob {}".format(
                block.id, self.blob_name), 'put_block')
            self.put_block_retry(self.container_name, self.blob_name,
                                 block_data.encode('ascii'), block.id)

        self.put_block_list(self.container_name, self.blob_name, blocks)

    def put_block_retry(self, *args, **kwargs):
        '''put_block with some retry - hotfix'''
        count = 0
        while count <= 5:
            try:
                self.put_block(*args, **kwargs)
                return None
            except AzureException as ex:
                logwriter.debug("raise AzureException", "put_block")
                count += 1
                sleep(1 * count)
        raise ex

    def _make_block(self, block_id):
        base64_block_id = self._b64encode(block_id)
        logwriter.debug("block id {} blockidbase64 {}"
                        .format(block_id, base64_block_id), '_make_block')
        return BlobBlock(id=base64_block_id)

    def _b64encode(self, i, length=8):
        i_str = '{0:0{length}d}'.format(i, length=length)
        b_ascii = i_str.encode('ascii')
        b_b64 = base64.b64encode(b_ascii)
        return b_b64.decode('ascii')

    def _gen_line_chunks(self, dataframe, n):
        a = 0
        b = n

        while a < len(dataframe):
            yield dataframe.iloc[a:b]
            a += n
            b += n
