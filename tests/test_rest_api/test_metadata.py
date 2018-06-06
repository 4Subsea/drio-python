import unittest
import datareservoirio
import datareservoirio.rest_api.metadata as metadata
from datareservoirio.rest_api import MetadataAPI

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock


dummy_meta = {
    "Namespace": "hello",
    "Key": "world",
    "Value": {'Norway': 'hei',
              'Klingon': 'grabah'}
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
        self.token = {'accessToken': 'abcdef'}
        self.metadata_id = 'metadata_id'

        self.api = MetadataAPI()
        mock_session = patch.object(self.api, '_session')
        mock_session.start()
        self.addCleanup(mock_session.stop)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_create(self, mock_token):
        mock_post = self.api._session.post

        self.api.create(self.token, 'hello', 'world',
                        Norway='hei', Klingon='grabah')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/create'

        expected_body = dummy_meta

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=expected_body, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_update(self, mock_token):
        mock_post = self.api._session.put

        self.api.update(self.token, self.metadata_id, 'hello', 'world',
                        Norway='hei', Klingon='grabah')

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}'.format(
            self.metadata_id)

        expected_body = dummy_meta

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=expected_body, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_get(self, mock_token):
        mock_post = self.api._session.get

        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        self.api.get(self.token, self.metadata_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}'.format(
            self.metadata_id)

        mock_post.assert_called_once_with(expected_uri, auth=mock_token(),
                                          **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_delete(self, mock_token):
        mock_post = self.api._session.delete

        self.api.delete(self.token, self.metadata_id)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/{}'.format(
            self.metadata_id)

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_search(self, mock_token):
        search_json = {
            "Namespace": "hello",
            "Key": "world",
            "Value": {},
            "Conjunctive": False
        }
        mock_post = self.api._session.post

        self.api.search(self.token, 'hello', 'world', conjunctive=False)

        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/search'

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=search_json, **self.api._defaults)

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_namespaces_returns_list(self, mock_token):
        mock_response = Mock()
        mock_response.json = Mock(return_value=['system_1.reservoir',
                                                'system_1.campaigns',
                                                'system_2.disco'])

        self.api._session.get.return_value = mock_response
        namespaces_out = self.api.namespaces(self.token)

        namespaces_expected = ['system_1', 'system_2']
        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/namespacekeys'

        self.api._session.get.assert_called_once_with(
            expected_uri, auth=mock_token(), **self.api._defaults)
        self.assertListEqual(namespaces_out, sorted(namespaces_expected))

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_keys_returns_list(self, mock_token):
        mock_response = Mock()
        mock_response.json = Mock(return_value=['system_1.reservoir',
                                                'system_1.campaigns',
                                                'system_2.disco'])

        self.api._session.get.return_value = mock_response
        keys_out = self.api.keys(self.token, 'system_1')

        keys_expected = ['reservoir', 'campaigns']
        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/namespacekeys'

        self.api._session.get.assert_called_once_with(
            expected_uri, auth=mock_token(), **self.api._defaults)
        self.assertListEqual(keys_out, sorted(keys_expected))

    @patch('datareservoirio.rest_api.metadata.TokenAuth')
    def test_names_returns_list(self, mock_token):
        mock_response = Mock()
        mock_response.json = Mock(return_value=['VesselName', 'CampaignName'])

        self.api._session.get.return_value = mock_response
        names_out = self.api.names(self.token, 'namesp', 'mykey')

        names_expected = ['VesselName', 'CampaignName']
        expected_uri = 'https://reservoir-api-qa.4subsea.net/api/metadata/namesp/mykey'

        self.api._session.get.assert_called_once_with(
            expected_uri, auth=mock_token(),  **self.api._defaults)
        self.assertListEqual(names_out, sorted(names_expected))


class Test__unpack_namespacekeys(unittest.TestCase):
    def test_unpack(self):
        namespacekeys = [
            'foo.bar',
            'animal.zebra',
            'car.part.wheel',
            None,
            'hello.world',
            'animal.lion'
        ]
        output_dict = metadata._unpack_namespacekeys(namespacekeys)
        expected_dict = {
            'foo': ['bar'],
            'animal': ['zebra', 'lion'],
            'car.part': ['wheel'],
            'hello': ['world']}

        self.assertDictEqual(output_dict, expected_dict)


class Test__assemble_metadata_json(unittest.TestCase):
    def test_assemble(self):
        output_dict = metadata._assemble_metadatajson(
            'hello', 'world', Norway='hei', Klingon='grabah')
        expected_dict = {
            'Namespace': 'hello',
            'Key': 'world',
            'Value': {'Norway': 'hei', 'Klingon': 'grabah'}
            }

        self.assertDictEqual(output_dict, expected_dict)


if __name__ == '__main__':
    unittest.main()
