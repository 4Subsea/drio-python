import logging

from .base import BaseAPI

log = logging.getLogger(__name__)


class MetadataAPI(BaseAPI):
    """
    Python wrapper for reservoir-api.4subsea.net/api/metadata

    Parameters
    ----------
    session :
        Authorized session instance (User or Client) which appends a valid bearer token to all
        HTTP calls.

    """

    def __init__(self, session):
        super(MetadataAPI, self).__init__(session)
        self._root = self._api_base_url + "metadata/"

    def delete(self, metadata_id):
        """
        Delete an existing metadata entry.

        Parameters
        ----------
        metadata_id : str
            id of metadata
        """
        log.debug(f"delete with <token>, {metadata_id}")

        uri = self._root + "{}".format(metadata_id)
        self._delete(uri)
        return

    def get_by_id(self, metadata_id):
        """
        Retrieve a metadata entry by its unique identifier.

        Parameters
        ----------
        metadata_id : str
            id of metadata

        Return
        ------
        dict
            response.json()
        """
        log.debug(f"get by id with <token>, {metadata_id}")

        uri = self._root + metadata_id
        response = self._get(uri)
        return response.json()

    def get(self, namespace, key):
        """
        Retrieve a metadata entry.

        Parameters
        ----------
        namespace : str
            Metadata namespace
        key : str
            Metadata key

        Return
        ------
        dict
            response.json()
        """
        log.debug(f"get with <token>, {namespace} {key}")

        uri = self._root + namespace + "/" + key
        response = self._get(uri)
        return response.json()

    def put_by_id(self, metadata_id, **namevalues):
        """
        Create or update an existing metadata entry. Overwrites `Value` object.

        Parameters
        ----------
        metadata_id : str
            id of metadata
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        log.debug(f"put by id with <token>, {metadata_id}, {namevalues}")

        metadata_json = {"Value": namevalues}

        uri = self._root + metadata_id
        response = self._put(uri, json=metadata_json)
        return response.json()

    def put(self, namespace, key, overwrite, **namevalues):
        """
        Create or update an existing metadata entry. Overwrites `Value` object.

        Parameters
        ----------
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
        log.debug(f"put with <token>, {namespace}, {key}, {overwrite}, {namevalues}")

        metadata_json = {"Value": namevalues}

        uri = self._root + "{}/{}?overwrite={}".format(
            namespace, key, "true" if overwrite else "false"
        )

        response = self._put(uri, json=metadata_json)
        return response.json()

    def namespaces(self):
        """
        Return a list of available metadata namespaces.

        Return
        ------
        list
            response.json()
        """
        uri = self._root
        response = self._get(uri)

        return sorted(response.json())

    def keys(self, namespace):
        """
        Return a list of available metadata keys under given namespace.

        Parameters
        ----------
        namespace : str
            Metadata namespace

        Return
        ------
        list
            response.json()
        """
        uri = self._root + namespace
        response = self._get(uri)

        return sorted(response.json())

    def search(self, namespace, key):
        """
        Search for metadata entries.

        Parameters
        ----------
        namespace : str
            Metadata namespace.
        key : str
            Metadata key.
        Return
        ------
        dict
            response.json()
        """
        log.debug(f"search with <token>, {namespace} {key}")

        search_json = _assemble_metadatajson(namespace, key)

        uri = self._root + "search"
        response = self._post(uri, json=search_json)
        return response.json()


def _assemble_metadatajson(namespace, key, **namevalues):
    """Assemble metadata json from input"""
    metadata_json = {"Namespace": namespace, "Key": key, "Value": namevalues}
    return metadata_json
