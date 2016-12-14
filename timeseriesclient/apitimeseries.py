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

    def create(self, token, file_id):
        logwriter.debug("called with <token>, {}".format(file_id), "create")

        uri = self._api_base_url + 'timeseries/create'
        headers = adalw.add_authorization_header({}, token)
        body = { "FileId":file_id }

        response = self._post(uri, headers=headers, data=body)
        logwriter.debug("status code: {}".format(response.status_code))
        logwriter.debug("raw body: {}".format(response.raw))
        logwriter.debug("raw text: {}".format(response.text))

        return json.loads(response.text)

    def add(self, token, timeseries_id, file_id):
        logwriter.debug("called with <token>, {}, {}".format(timeseries_id, file_id), "add")

        uri = self._api_base_url + 'timeseries/add'
        headers = adalw.add_authorization_header({}, token)
        body = { "TimeSeriesId":timeseries_id, "FileId":file_id }

        response = self._post(uri, headers=headers, data=body)
        logwriter.debug("status code: {}".format(response.status_code))
        logwriter.debug("raw body: {}".format(response.raw))
        logwriter.debug("raw text: {}".format(response.text))

        return json.loads(response.text)

    def list(self, token):
        logwriter.debug("called with <token>")

        uri = self._api_base_url + 'TimeSeries/list'
        headers = adalw.add_authorization_header({}, token)

        response = self._get(uri, headers=headers)

        return json.loads(response.text)

    def info(self, token, timeseries_id):
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        headers = adalw.add_authorization_header({}, token)

        response = self._get(uri, headers=headers)

        return json.loads(response.text)
    

    def delete(self, token, timeseries_id):
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        headers = adalw.add_authorization_header({}, token)

        response = self._delete(uri, headers=headers)

        return

    def get(self, token, timeseries_id, start, stop):
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/{}/data'.format(timeseries_id)
        headers = adalw.add_authorization_header({}, token)
        # create parameters from start and stop to be sent with request
        # parameters = ...

        response = self._get(uri, headers=headers)

        # return stream to client...
        #return json.loads(response.text)
        return


    def _get(self, uri, headers, member=None):
        logwriter.debug("issued get request to {}".format(uri))
        response = requests.get(uri, headers=headers)
        logwriter.debug("response status code: {}".format(response.status_code))
        logwriter.debug("response text: {}".format(response.text))
        return response

    def _delete(self, uri, headers, member=None):
        logwriter.debug("issued delete request to {}".format(uri))
        response = requests.delete(uri, headers=headers)
        logwriter.debug("response status code: {}".format(response.status_code))
        logwriter.debug("response text: {}".format(response.text))
        return response

    def _post(self, uri, headers, data=None, member=None):
        logwriter.debug("issued post request to {}".format(uri), member)
        response = requests.post(uri, headers=headers, data=data)
        logwriter.debug("response status code: {}".format(response.status_code), member)
        logwriter.debug("received: {}".format(response.text), member)

        return response


class TimeSeriesApiMock(object):

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

        self.create_return_value = { "TimeSeriesId", 'abcde' }
        self.list_return_value = ['ts1', 'ts2', 'ts3']

    def create(self, token, file_id, reference_time):
        self.last_token = token
        return self.create_return_value

    def add(self, token, timeseries_id, file_id):
        return { 'TimeSeriesId':timeseries_id, 'FileId':file_id }

    def list(self, token):
        self.last_token = token 
        return self.list_return_value 

    def info(self, token, timeseries_id):
        self.last_token = token
        return {"TimeSeriesId": timeseries_id }


    def delete(self, token, timeseries_id):
        self.last_token = token

    def get(self, timeseries_id, start, stop):
        self.last_timeseries_id = timeseries_id
        

