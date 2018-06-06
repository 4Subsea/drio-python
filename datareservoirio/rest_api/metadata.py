from __future__ import absolute_import, division, print_function

import logging
from collections import defaultdict

from ..log import LogWriter
from .base import BaseAPI, TokenAuth

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class MetadataAPI(BaseAPI):
    """Python wrapper for reservoir-api.4subsea.net/api/metadata"""

    def __init__(self, session=None):
        super(MetadataAPI, self).__init__(session=session)

    def create(self, token, namespace, key, **namevalues):
        """
        Create a metadata entry.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        namespace : str
            Metadata namespace
        key : str
            Metadata key
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}, {}".format(
            namespace, key, namevalues), "create")

        metadata_json = _assemble_metadatajson(namespace, key, **namevalues)

        uri = self._api_base_url + 'metadata/create'
        response = self._post(uri, json=metadata_json, auth=TokenAuth(token))
        return response.json()

    def update(self, token, metadata_id, namespace, key, **namevalues):
        """
        Update an existing metadata entry. Overwrites `Value` object.

        Parameters
        ----------
        token : dict
            token recieved from authenticator
        metadata_id : str
            id of metadata
        namespace : str
            Metadata namespace
        key : str
            Metadata key
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        logwriter.debug("called with <token>, {}, {}, {}".format(
            namespace, key, namevalues), "update")

        metadata_json = _assemble_metadatajson(namespace, key, **namevalues)

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
        logwriter.debug('called with <token>, {}'.format(
            metadata_id), 'delete')

        uri = self._api_base_url + 'metadata/{}'.format(metadata_id)
        self._delete(uri, auth=TokenAuth(token))
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
        logwriter.debug('called with <token>, {}'.format(metadata_id), u'get')

        uri = self._api_base_url + 'metadata/' + metadata_id
        response = self._get(uri, auth=TokenAuth(token))
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

        Note
        ----
        This is an temporary implementation. Most of the logic should happend
        server side.
        """
        uri = self._api_base_url + 'metadata/namespacekeys'
        response = self._get(uri, auth=TokenAuth(token))

        namespacekeys = _unpack_namespacekeys(response.json())  # temporary solution
        namespaces = [ns for ns in namespacekeys.keys()]
        return sorted(namespaces)

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

        Note
        ----
        This is an temporary implementation. Most of the logic should happend
        server side.
        """
        uri = self._api_base_url + 'metadata/namespacekeys'
        response = self._get(uri, auth=TokenAuth(token))

        namespacekeys = _unpack_namespacekeys(response.json())  # temporary solution
        keys = namespacekeys.get(namespace, [])
        return sorted(keys)

    def names(self, token, namespace, key):
        """
        Return a list of metadata names given namespace and key.

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
        list
            response.json()
        """
        uri = self._api_base_url + 'metadata/' + namespace + '/' + key
        response = self._get(uri, auth=TokenAuth(token))
        return sorted(response.json())

    def search(self, token, namespace, key, conjunctive=True):
        """
        TODO: Extend functionality for more powerful search experience.

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

        uri = self._api_base_url + 'metadata/search'
        response = self._post(uri, json=search_json, auth=TokenAuth(token))
        return response.json()


def _unpack_namespacekeys(namespacekeys):
    """Parse and unpack namespacekeys into dict -> {namespace: key}"""
    namespacekeys_dict = defaultdict(list)
    for nskey in namespacekeys:
        if nskey is None:
            continue
        namespace_key = nskey.split('.')
        namespace = '.'.join(namespace_key[0:-1])
        key = namespace_key[-1]
        namespacekeys_dict[namespace].append(key)
    return namespacekeys_dict


def _assemble_metadatajson(namespace, key, **namevalues):
    """Assemble metadata json from input"""
    metadata_json = {
        'Namespace': namespace,
        'Key': key,
        'Value': namevalues}
    return metadata_json
