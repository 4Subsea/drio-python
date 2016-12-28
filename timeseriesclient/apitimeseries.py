import requests
import json
import logging

from . import globalsettings

from . import adalwrapper as adalw
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class TimeSeriesApi(object):
    """
    Python wrapper for reservoir-api.4subsea.net/api/timeseries
    """    

    def __init__(self):
        self._api_base_url = globalsettings.environment.api_base_url

    def create(self, token, file_id):
        """
        Create timeseries entry in the reservoir

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        file_id : str
            id of file (File API) to be tied to timeseries entry.

        Return
        ------
        dict
            http response.text loaded as json
        """
        logwriter.debug("called with <token>, {}".format(file_id), "create")

        uri = self._api_base_url + 'timeseries/create'
        headers = adalw.add_authorization_header({}, token)
        body = { "FileId":file_id }

        response = self._post(uri, headers=headers, data=body)
        return json.loads(response.text)

    def add(self, token, timeseries_id, file_id):
        """
        Append timeseries data to an existing entry in the reservoir

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries
        file_id : str
            id of file (File API) to be appended.

        Notes
        -----
        Refer to API documentation wrt apppend, overlap, and overwrite behavior
        """
        logwriter.debug("called with <token>, {}, {}".format(timeseries_id, file_id), "add")

        uri = self._api_base_url + 'timeseries/add'
        headers = adalw.add_authorization_header({}, token)
        body = { "TimeSeriesId":timeseries_id, "FileId":file_id }

        response = self._post(uri, headers=headers, data=body)
        return json.loads(response.text)

    def list(self, token):
        """
        List all existing entries in the reservoir

        Parameters
        ----------
        token : dict
            token recieved from authenticator

        Return
        ------
        list of dict
            list of dictionaries containing information about timeseries
            entries in the reservoir

        See also
        -----
        TimeSeriesApi.info()
        """
        logwriter.debug("called with <token>")

        uri = self._api_base_url + 'TimeSeries/list'
        headers = adalw.add_authorization_header({}, token)

        response = self._get(uri, headers=headers)
        return json.loads(response.text)

    def info(self, token, timeseries_id):
        """
        Information about a timeseries entry in the reservoir

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries

        Return
        ------
        dict
            dictionary containing information about a timeseries
            entry in the reservoir
        """
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        headers = adalw.add_authorization_header({}, token)

        response = self._get(uri, headers=headers)
        return json.loads(response.text)

    def delete(self, token, timeseries_id):
        """
        Delete a timeseries from the reservoir

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries
        """
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        headers = adalw.add_authorization_header({}, token)

        response = self._delete(uri, headers=headers)
        return

    def data(self, token, timeseries_id, start, end):
        """
        Return timeseries data with start/stop from reservoir.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the timeseries to download
        start : int long
            start time in nano seconds since epoch.
        end : int long
            end time in nano seconds since epoch.

        Return
        ------
        str
            csv with timeseries data
        """
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/{}/data'.format(timeseries_id)
        headers = adalw.add_authorization_header({}, token)
        params = {'start': start, 'end': end}

        response = self._get(uri, headers=headers, params=params)
        return response

    def _get(self, *args, **kwargs):
        response = requests.get(*args, **kwargs)
        logwriter.debug("response request url: {}".format(response.request.url))
        logwriter.debug("response status code: {}".format(response.status_code))
        logwriter.debug("response text: {}".format(response.text))
        return response

    def _delete(self, *args, **kwargs):
        response = requests.delete( *args, **kwargs)
        logwriter.debug("response request url: {}".format(response.request.url))
        logwriter.debug("response status code: {}".format(response.status_code))
        logwriter.debug("response text: {}".format(response.text))
        return response

    def _post(self, *args, **kwargs):
        response = requests.post(*args, **kwargs)
        logwriter.debug("response request url: {}".format(response.request.url))
        logwriter.debug("response status code: {}".format(response.status_code))
        logwriter.debug("response text: {}".format(response.text))
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
