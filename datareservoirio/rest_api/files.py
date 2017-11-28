from __future__ import absolute_import

import logging

from ..log import LogWriter
from ..storage_engine import AzureBlobService
from .base import BaseAPI, TokenAuth

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
    def transfer_service(account_params):
        """
        Initiate an data transfer service.

        An instance of a subclass of Azure Storage BlockBlobService is
        returned.

        Parameters
        ----------
        account_params : dict
            parameters recieved by e.g. `upload` method

        Return
        ------
        object
            Pre-configured transfer service
        """

        logwriter.debug("called <token>, <upload_params>", "upload_service")
        return AzureBlobService(account_params)

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