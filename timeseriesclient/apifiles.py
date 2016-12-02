import requests
import json
import logging

from . import globalsettings
from . import adalwrapper as adalw
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class FilesApi(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url
    
    def upload(self, token):
        logwriter.debug("called", "upload")

        uri = self._api_base_url + 'Files/upload'
        headers = adalw.add_authorization_header({}, token)

        response = self._post(uri, headers=headers, member='upload')
        
        upload_params = json.loads(response.text)

        #upload_params['SasKey'] = upload_params['SasKey'].lstrip('?')    

        return upload_params

    def commit(self, token, file_id):
        uri = self._api_base_url + 'Files/commit'
        headers = adalw.add_authorization_header({}, token)
        body = { 'FileId' : file_id }

        response = self._post(uri, headers=headers, data=body)

        return response.status_code

    def _post(self, uri, headers, data=None, member=None):
        logwriter.debug("issued post request to {}".format(uri), member)
        response = requests.post(uri, headers=headers, data=data)
        logwriter.debug("response status code: {}".format(response.status_code), member)
        logwriter.debug("received: {}".format(response.text), member)

        return response


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

