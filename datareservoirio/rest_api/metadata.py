from __future__ import absolute_import, division, print_function

import logging

from ..log import LogWriter
from .base import BaseAPI, TokenAuth

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class MetadataAPI(BaseAPI):
    """Python wrapper for reservoir-api.4subsea.net/api/metadata"""

    def __init__(self, session=None):
        super(MetadataAPI, self).__init__(session=session)
        self._root = self._api_base_url + 'metadata/'

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
        logwriter.debug('called with <token>, {}'.format(
            metadata_id), 'delete')

        uri = self._root + '{}'.format(metadata_id)
        self._delete(uri, auth=TokenAuth(token))
        return

    def get_by_id(self, token, metadata_id):
        """
        Retrieve a metadata entry by its unique identifier.

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
        logwriter.debug('called with <token>, {}'.format(metadata_id), u'get')

        uri = self._root + metadata_id
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def get(self, token, namespace, key):
        """
        Retrieve a metadata entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        namespace : str
            Metadata namespace
        key : str
            Metadata key

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug('called with <token>, {} {}'.format(
            namespace, key), u'get')

        uri = self._root + namespace + '/' + key
        response = self._get(uri, auth=TokenAuth(token))
        return response.json()

    def put_by_id(self, token, metadata_id, **namevalues):
        """
        Create or update an existing metadata entry. Overwrites `Value` object.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}".format(
            metadata_id, namevalues), "put")

        metadata_json = {'Value': namevalues}

        uri = self._root + metadata_id
        response = self._put(uri, json=metadata_json, auth=TokenAuth(token))
        return response.json()

    def put(self, token, namespace, key, overwrite, **namevalues):
        """
        Create or update an existing metadata entry. Overwrites `Value` object.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        namespace : str
            Metadata namespace
        key : str
            Metadata key
        overwrite : bool
            True if any existing metadata should be overwritten. If false, an error will be
            thrown if the namespace/key combination already exists.
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}, {}, {}".format(
            namespace, key, overwrite, namevalues), "put")

        metadata_json = {'Value': namevalues}

        uri = self._root + '{}/{}?overwrite={}'.format(
            namespace, key, 'true' if overwrite else 'false')

        response = self._put(uri, json=metadata_json, auth=TokenAuth(token))
        return response.json()

    def namespaces(self, token):
        """
        Return a list of available metadata namespaces.

        Parameters
        ----------
        token : dict
            token recieved from authenticator

        Return
        ------
        list
            response.json()
        """
        uri = self._root
        response = self._get(uri, auth=TokenAuth(token))

        return sorted(response.json())

    def keys(self, token, namespace):
        """
        Return a list of available metadata keys under given namespace.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        namespace : str
            Metadata namespace

        Return
        ------
        list
            response.json()
        """
        uri = self._root + namespace
        response = self._get(uri, auth=TokenAuth(token))

        return sorted(response.json())

    def search(self, token, namespace, key, conjunctive=True):
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
        logwriter.debug("called with <token>, {} {} {}".format(
            namespace, key, conjunctive), 'search')

        search_json = _assemble_metadatajson(namespace, key)
        search_json['Conjunctive'] = conjunctive

        uri = self._root + 'search'
        response = self._post(uri, json=search_json, auth=TokenAuth(token))
        return response.json()


def _assemble_metadatajson(namespace, key, **namevalues):
    """Assemble metadata json from input"""
    metadata_json = {
        'Namespace': namespace,
        'Key': key,
        'Value': namevalues}
    return metadata_json
