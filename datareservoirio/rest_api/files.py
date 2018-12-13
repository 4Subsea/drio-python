from __future__ import absolute_import, division, print_function

import logging

from ..log import LogWriter
from .base import BaseAPI

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class FilesAPI(BaseAPI):
    """
    Python wrapper for reservoir-api.4subsea.net/api/files.

    Parameters
    ----------
    session : subclass of ``requests.session``
        Authorized session instance which appends a valid bearer token to all
        HTTP calls.

    """
    def __init__(self, session):
        super(FilesAPI, self).__init__(session)

    def upload(self):
        """
        Create file entry in the reservoir.

        Return
        ------
        dict
            Parameters requried by the uploader service
        """

        logwriter.debug('called with <token>', 'upload')

        uri = self._api_base_url + 'Files/upload'
        response = self._post(uri)

        for key, value in response.json().items():
            logwriter.debug('parameter received - {key}: {value}'
                            .format(key=key, value=value), 'upload')
        return response.json()

    def commit(self, file_id):
        """
        Commit a file.

        Parameters
        ----------
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
        response = self._post(uri, data=body)
        return response.status_code

    def status(self, file_id):
        """
        Probe file status.

        Parameters
        ----------
        file_id : str
            id of file (Files API) to be commit.

        Return
        ------
        str
            'Unitialized', 'Processing', 'Ready', or 'Failed'
        """
        logwriter.debug('called with <token>, {}'.format(file_id), 'status')

        uri = self._api_base_url + 'files/{}/status'.format(file_id)
        response = self._get(uri)
        return response.json()

    def ping(self):
        """
        Ping server.

        Return
        ------
        dict
            pong
        """
        logwriter.debug('called <token>', 'ping')

        uri = self._api_base_url + 'ping'
        response = self._get(uri)
        return response.json()
