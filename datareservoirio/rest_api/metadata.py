from __future__ import absolute_import, division, print_function

import logging

from ..log import LogWriter
from .base import BaseAPI, TokenAuth

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class MetadataAPI(BaseAPI):
    """Python wrapper for reservoir-api.4subsea.net/api/metadata"""

    def __init__(self):
        super(MetadataAPI, self).__init__()

    def namespacekeys(self, token):
        """
        Return a list of available metadata namespace/key combinations

        Parameters
        ----------
        token : dict
            token recieved from authenticator

        Return
        ------
        list
            response.json()
        """
        uri = self._api_base_url + 'metadata/namespacekeys'
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def metadata(self, token, namespace, key):
        """
        Return a list of names in metadata value-json with given
        namespace and key

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        namespace : string
            the metadata namespace
        key : string
            the key in the namespace

        Return
        ------
        list
            response.json()
        """
        uri = self._api_base_url + 'metadata/' + namespace + '/' + key
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def create(self, token, metadata_json):
        """
        Create a metadata entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_json : dict
            Dict with metadata. Must contain "Namespace" (str), "Key" (str),
            and "Value" (dict). Subjected to validation before passed on.

        Return
        ------
        dict
            response.json()
        """
        # json validation in here
        logwriter.debug("called with <token>, {}".format(
            metadata_json), "create")

        uri = self._api_base_url + 'metadata/create'
        response = self._post(uri, json=metadata_json, auth=TokenAuth(token))
        return response.json()

    def update(self, token, metadata_id, metadata_json):
        """
        Update an existing metadata entry. Overwrites `Value` object.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata
        metadata_json : dict
            Dict with metadata. Must contain "Namespace" (str), "Key" (str),
            and "Value" (dict). Subjected to validation before passed on.

        Return
        ------
        dict
            response.json()
        """
        # json validation in here
        logwriter.debug("called with <token>, {}, {}".format(
            metadata_id, metadata_json), "reset")

        uri = self._api_base_url + 'metadata/{}'.format(metadata_id)
        response = self._put(uri, json=metadata_json, auth=TokenAuth(token))
        return response.json()

    def delete(self, token, metadata_id):
        """
        Delete an existing metadata entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata
        """
        logwriter.debug("called with <token>, {}".format(
            metadata_id), "delete")

        uri = self._api_base_url + 'metadata/{}'.format(metadata_id)
        response = self._delete(uri, auth=TokenAuth(token))
        return

    def get(self, token, metadata_id):
        """
        Retrieve a metadata entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}".format(metadata_id), 'get')

        uri = self._api_base_url + 'metadata/' + metadata_id
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def list(self, token):
        """
        Retrieve a list of all metadata entries.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata

        Return
        ------
        list of dicts
            response.json()
        """
        logwriter.debug("called with <token>", 'list')

        uri = self._api_base_url + 'metadata/list'
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def search(self, token, search_json):
        """
        Search for metadata entries.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        search_json : dict
            Dict with search data. Must contain "Namespace" (str), "Key" (str),
            and "Conjunctive" (bool). Subjected to validation before passed on.

        Return
        ------
        dict
            response.json()
        """
        # json validation here
        logwriter.debug("called with <token>, {}".format(
            search_json), 'search')

        uri = self._api_base_url + 'metadata/search'
        response = self._post(uri, json=search_json, auth=TokenAuth(token))
        return response.json()

    def attach_series(self, token, metadata_id, series_id_list):
        """
        Attach a metadata entry to a list of series.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata
        series_id_list : list
            list of series_id (str)

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            metadata_id, series_id_list), "attach_series")

        uri = self._api_base_url + \
            'metadata/{}/attachTimeSeries'.format(metadata_id)

        response = self._post(uri, json=series_id_list, auth=TokenAuth(token))
        return response.json()

    def detach_series(self, token, metadata_id, series_id_list):
        """
        Detach a metadata entry from a list of series.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata
        series_id_list : list
            list of series_id (str)

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            metadata_id, series_id_list), "detach_series")

        uri = self._api_base_url + \
            'metadata/{}/detachTimeSeries'.format(metadata_id)

        response = self._delete(uri, json=series_id_list,
                                auth=TokenAuth(token))
        return response.json()
