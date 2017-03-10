from __future__ import absolute_import

import logging

from .. import globalsettings
from ..log import LogWriter
from .base import BaseAPI, TokenAuth

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class TimeSeriesAPI(BaseAPI):
    """Python wrapper for reservoir-api.4subsea.net/api/timeseries"""

    def __init__(self):
        super(TimeSeriesAPI, self).__init__()

    def create(self, token, file_id):
        """
        Create timeseries entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        file_id : str
            id of file (Files API) to be tied to timeseries entry.

        Return
        ------
        dict
            http response.text loaded as json
        """
        logwriter.debug("called with <token>, {}".format(file_id), "create")

        uri = self._api_base_url + 'timeseries/create'
        body = {"FileId": file_id}
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.json()

    def add(self, token, timeseries_id, file_id):
        """
        Append timeseries data to an existing entry.

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
        logwriter.debug("called with <token>, {}, {}".format(
            timeseries_id, file_id), "add")

        uri = self._api_base_url + 'timeseries/add'
        body = {"TimeSeriesId": timeseries_id, "FileId": file_id}
        response = self._post(uri, data=body, auth=TokenAuth(token))
        return response.json()

    def list(self, token):
        """
        List all existing entries.

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
        TimeSeriesAPI.info()
        """
        logwriter.debug("called with <token>")

        uri = self._api_base_url + 'TimeSeries/list'
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def info(self, token, timeseries_id):
        """
        Information about a timeseries entry.

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
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def delete(self, token, timeseries_id):
        """
        Delete a timeseries.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of the target timeseries
        """
        logwriter.debug("called with <token>, {}".format(timeseries_id))

        uri = self._api_base_url + 'timeseries/' + timeseries_id
        response = self._delete(uri, auth=TokenAuth(token))
        return

    def data(self, token, timeseries_id, start, end):
        """
        Return timeseries data with start/stop.

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
        logwriter.debug("called with <token>, {}, {}, {}".format(
            timeseries_id, start, end))

        uri = self._api_base_url + 'timeseries/{}/data'.format(timeseries_id)
        params = {'start': start, 'end': end}

        response = self._get(uri, params=params, auth=TokenAuth(token))
        return response

    def attach_metadata(self, token, timeseries_id, metadata_id_list):
        """
        Attach a list of metadata entries to a series.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of timeseries
        metadata_id_list : list
            list of metadata_id

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            timeseries_id, metadata_id_list), "attach_metadata")

        uri = self._api_base_url + \
            'timeseries/{}/attachMetadata'.format(timeseries_id)

        response = self._post(uri, json=metadata_id_list,
                              auth=TokenAuth(token))
        return response.json()

    def detach_metadata(self, token, timeseries_id, metadata_id_list):
        """
        Detach a list of metadata entries from a timeseries.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        timeseries_id : str
            id of timeseries
        metadata_id_list : list
            list of metadata_id

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            timeseries_id, metadata_id_list), "attach_metadata")

        uri = self._api_base_url + \
            'timeseries/{}/detachMetadata'.format(timeseries_id)

        response = self._delete(uri, json=metadata_id_list,
                                auth=TokenAuth(token))
        return response.json()
