from __future__ import absolute_import, division, print_function

import logging

from ..log import LogWriter
from .base import BaseAPI, TokenAuth

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class FilesAPI(BaseAPI):
    """Python wrapper for reservoir-api.4subsea.net/api/files."""

    def __init__(self, session=None):
        super(FilesAPI, self).__init__(session=session)

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

        logwriter.debug('called with <token>', 'upload')

        uri = self._api_base_url + 'Files/upload'
        response = self._post(uri, auth=TokenAuth(token))

        for key, value in response.json().items():
            logwriter.debug('parameter received - {key}: {value}'
                            .format(key=key, value=value), 'upload')
        return response.json()

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
        logwriter.debug('called with <token>, {}'.format(file_id), 'commit')

        uri = self._api_base_url + 'Files/commit'
        body = {'FileId': file_id}
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.status_code

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
            'Unitialized', 'Processing', 'Ready', or 'Failed'
        """
        logwriter.debug('called with <token>, {}'.format(file_id), 'status')

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
        logwriter.debug('called <token>', 'ping')

        uri = self._api_base_url + 'ping'
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()
