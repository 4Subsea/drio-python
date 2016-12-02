import requests
import json
import logging

from . import globalsettings
from . import adalwrapper as adalw
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

class TimeSeriesApi(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

    def create(self, token, file_id, reference_time):
        logwriter.debug("called with {}, {}, {}".format(token, file_id, reference_time), "create")

        uri = self._api_base_url + 'TimeSeries/create'
        headers = adalw.add_authorization_header({}, token)
        body = { "FileId":file_id, "ReferenceTime":reference_time }

        response = requests.post(uri, headers=headers, data=body)

        return json.loads(response.text)

    def append(self, token, timeseries_id, file_id):
        logwriter.debug("called with {}, {}, {}".format(token, timeseries_id, file_id), "append")

        uri = self._api_base_url + 'TimeSeries/append_to'
        headers = adalw.add_authorization_header({}, token)
        body = { "TimeSeriesId":timeseries_id, "FileId":file_id }

        response = requests.post(uri, headers=headers, data=body)

        return json.loads(response.text)

    def list(self, token):
        logwriter.debug("called with {}".format(token))

        uri = self._api_base_url + 'TimeSeries/list'
        headers = adalw.add_authorization_header({}, token)

        response = requests.get(uri, headers=headers)

        return json.loads(response.text)

    def delete(self, token, timeseries_id):
        logwriter.debug("called with {}, {}".format(token, timeseries_id))

        uri = self._api_base_url + 'TimeSeries/delete/' + timeseries_id
        headers = adalw.add_authorization_header({}, token)

        response = requests.delete(uri, headers=headers)

        return json.loads(response.text)


class TimeSeriesApiMock(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

        self.create_return_value = { "TimeSeriesId", 'abcde' }
        self.list_return_value = ['ts1', 'ts2', 'ts3']

    def create(self, token, file_id, reference_time):
        self.last_token = token
        return self.create_return_value

    def append(self, token, timeseries_id, file_id):
        return { 'TimeSeriesId':timeseries_id, 'FileId':file_id }

    def list(self, token):
        self.last_token = token 
        return self.list_return_value 

    def delete(self, token, timeseries_id):
        self.last_token = token
        

