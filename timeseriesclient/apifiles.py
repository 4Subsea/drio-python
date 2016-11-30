import requests
import json

from . import globalsettings
from . import adalwrapper as adalw

class FilesApi(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url
    
    def upload(self, token):
        uri = self._api_base_url + 'Files/upload'
        headers = adalw.add_authorization_header({}, token)

        response = requests.post(uri, headers=headers)
        upload_params = json.loads(response.text)

        #upload_params['SasKey'] = upload_params['SasKey'].lstrip('?')    

        return upload_params

    def commit(self, token, file_id):
        uri = self._api_base_url + 'Files/commit'
        headers = adalw.add_authorization_header({}, token)
        body = { 'FileId' : file_id }

        response = requests.post(uri, headers=headers, data=body)

        return response.status_code


class FilesApiMock(object):

    def upload(self, token):
        pass
