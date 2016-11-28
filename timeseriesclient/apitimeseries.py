import requests
import json

from . import globalsettings
from . import adalwrapper as adalw

class TimeSeriesApi(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

    def list(self, token):
        uri = self._api_base_url + 'TimeSeries/list'
        headers = adalw.add_authorization_header({}, token)

        response = requests.get(uri, headers=headers)

        return json.loads(response.text)

    def delete(self, token, timeseries_id):
        uri = self._api_base_url + 'TimeSeries/delete/' + timeseries_id
        headers = adalw.add_authorization_header({}, token)

        response = requests.get(uri, headers=headers)

        return json.loads(response.text)


class TimeSeriesApiMock(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

        self.list_return_value = ['ts1', 'ts2', 'ts3']

    def list(self, token):
        self.last_token = token 
        return self.list_return_value 

