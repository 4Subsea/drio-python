import requests
import json
import logging

from . import globalsettings
from . import adalwrapper as adalw
from .log import LogWriter
from .apiwebbase import WebBaseApi

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class FilesApi(WebBaseApi):

    def __init__(self):
        super(FilesApi, self).__init__()
    
    def upload(self, token):
        logwriter.debug("called", "upload")

        uri = self._api_base_url + 'Files/upload'
        headers = adalw.add_authorization_header({}, token)

        response = self._post(uri, headers=headers)
        upload_params = json.loads(response.text)
        return upload_params

    def commit(self, token, file_id):
        logwriter.debug("called", "commit")

        uri = self._api_base_url + 'Files/commit'
        headers = adalw.add_authorization_header({}, token)
        body = { 'FileId' : file_id }

        response = self._post(uri, headers=headers, data=body)
        return response.status_code

    def status(self, token, file_id):
        logwriter.debug("called", "status")

        uri = self._api_base_url + 'files/{}/status'.format(file_id)
        headers = adalw.add_authorization_header({}, token)

        response = self._get(uri, headers=headers)
        return json.loads(response.text)

    def ping(self, token):
        logwriter.debug("called", "ping")

        uri = self._api_base_url + 'ping'
        headers = adalw.add_authorization_header({}, token)

        response = self._get(uri, headers=headers)
        return json.loads(response.text)


class FilesApiMock(object):

    def upload(self, token):
        dummy_params = { 
            'FileId' : 666,
            'Account' : 'account',
            'SasKey' : 'abcdef',
            'Container' : 'blobcontainer', 
            'Path' : 'blobpath',
            'Endpoint' : 'endpointURI' 
        }
        
        return dummy_params

    def commit(self, token, file_id):
        logwriter.debug("called, will return status code 200", 'commit') 
        return 200

    def ping(self, token):
        return {'status':'pong'}

