import logging

from .base import BaseAPI

log = logging.getLogger(__name__)


class FilesAPI(BaseAPI):
    """
    Python wrapper for reservoir-api.4subsea.net/api/files.

    Parameters
    ----------
    session :
        Authorized session instance (User or Client) which appends a valid bearer token to all
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

        log.debug("upload with <token>")

        uri = self._api_base_url + "files/upload"
        response = self._post(uri)

        if log.isEnabledFor(logging.DEBUG):
            for key, value in response.json().items():
                log.debug(f"parameter received - {key}: {value}")

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
        log.debug(f"commit with <token>, {file_id}")

        uri = self._api_base_url + "files/commit"
        body = {"FileId": file_id}
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
        log.debug(f"status with <token>, {file_id}")

        uri = self._api_base_url + "files/{}/status".format(file_id)
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
        log.debug("ping with <token>")

        uri = self._api_base_url + "ping"
        response = self._get(uri)
        return response.json()
