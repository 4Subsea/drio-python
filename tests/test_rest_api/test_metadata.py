import unittest
from unittest.mock import Mock

import datareservoirio
import datareservoirio.rest_api.metadata as metadata
from datareservoirio.rest_api import MetadataAPI

dummy_meta = {
    "Namespace": "hello",
    "Key": "world",
    "Value": {"Norway": "hei", "Klingon": "grabah"},
}

dummy_response = """
    {
    "Id": "string",
    "Namespace": "string",
    "Key": "string",
    "Value": {},
    "TimeSeries": [
        {
        "TimeSeriesId": "string",
        "TimeOfFirstSample": 0,
        "TimeOfLastSample": 0,
        "LastModifiedByEmail": "string",
        "Created": "2017-03-01T13:28:39.180Z",
        "LastModified": "2017-03-01T13:28:39.180Z",
        "CreatedByEmail": "string",
        "Metadata": [
            {}
        ]
        }
    ],
    "LastModifiedByEmail": "string",
    "LastModified": "2017-03-01T13:28:39.180Z",
    "Created": "2017-03-01T13:28:39.180Z",
    "CreatedByEmail": "string"
    }"""


def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_MetadataAPI(unittest.TestCase):
    def setUp(self):
        self.token = {"accessToken": "abcdef"}
        self.metadata_id = "metadata_id"

        mock_session = Mock()

        self.api = MetadataAPI(mock_session)
        self.api._root = "https://root/metadata/"

    def test_get_by_id(self):
        mock_post = self.api._session.get

        mock_post.return_value = Mock()
        mock_post.return_value.text = u"{}"

        self.api.get_by_id(self.metadata_id)

        expected_uri = "https://root/metadata/{}".format(self.metadata_id)

        mock_post.assert_called_once_with(expected_uri, **self.api._defaults)

    def test_get_by_namespace_and_key(self):
        mock_post = self.api._session.get

        mock_post.return_value = Mock()
        mock_post.return_value.text = u"{}"

        self.api.get("hello", "world")

        expected_uri = "https://root/metadata/{}/{}".format("hello", "world")

        mock_post.assert_called_once_with(expected_uri, **self.api._defaults)

    def test_put_by_id(self):
        mock_post = self.api._session.put

        self.api.put_by_id(self.metadata_id, Norway="hei", Klingon="grabah")

        expected_uri = "https://root/metadata/{}".format(self.metadata_id)
        expected_body = {"Value": {"Norway": "hei", "Klingon": "grabah"}}

        mock_post.assert_called_with(
            expected_uri, json=expected_body, **self.api._defaults
        )

    def test_put_by_namespace_and_key(self):
        mock_post = self.api._session.put

        self.api.put("hello", "world", True, Norway="hei", Klingon="grabah")

        expected_uri = "https://root/metadata/{}/{}?overwrite={}".format(
            "hello", "world", "true"
        )
        expected_body = {"Value": {"Norway": "hei", "Klingon": "grabah"}}

        mock_post.assert_called_with(
            expected_uri, json=expected_body, **self.api._defaults
        )

    def test_delete(self):
        mock_post = self.api._session.delete

        self.api.delete(self.metadata_id)

        expected_uri = "https://root/metadata/{}".format(self.metadata_id)

        mock_post.assert_called_with(expected_uri, **self.api._defaults)

    def test_search(self):
        search_json = {
            "Namespace": "hello",
            "Key": "world",
            "Value": {},
            "Conjunctive": False,
        }
        mock_post = self.api._session.post

        self.api.search("hello", "world", conjunctive=False)

        expected_uri = "https://root/metadata/search"

        mock_post.assert_called_with(
            expected_uri, json=search_json, **self.api._defaults
        )

    def test_namespaces_returns_list(self):
        mock_response = Mock()
        mock_response.json = Mock(return_value=["system_1", "system_2"])

        self.api._session.get.return_value = mock_response
        namespaces_out = self.api.namespaces()

        namespaces_expected = ["system_1", "system_2"]
        expected_uri = "https://root/metadata/"

        self.api._session.get.assert_called_once_with(
            expected_uri, **self.api._defaults
        )
        self.assertListEqual(namespaces_out, sorted(namespaces_expected))

    def test_keys_returns_list(self):
        mock_response = Mock()
        mock_response.json = Mock(return_value=["reservoir", "campaigns"])

        self.api._session.get.return_value = mock_response
        keys_out = self.api.keys("system_1")

        keys_expected = ["reservoir", "campaigns"]
        expected_uri = "https://root/metadata/system_1"

        self.api._session.get.assert_called_once_with(
            expected_uri, **self.api._defaults
        )
        self.assertListEqual(keys_out, sorted(keys_expected))


class Test__assemble_metadata_json(unittest.TestCase):
    def test_assemble(self):
        output_dict = metadata._assemble_metadatajson(
            "hello", "world", Norway="hei", Klingon="grabah"
        )
        expected_dict = {
            "Namespace": "hello",
            "Key": "world",
            "Value": {"Norway": "hei", "Klingon": "grabah"},
        }

        self.assertDictEqual(output_dict, expected_dict)


if __name__ == "__main__":
    unittest.main()
