import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


class Test_ClientMetadata(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

    def setUp(self):
        self.client = datareservoirio.Client(self.auth, cache=False)

    def tearDown(self):
        self.client.__exit__()

    def test_metadata_set_with_data_returns_id_of_existing_metadata(self):
        response = self.client.metadata_set(
            'system.integrationtest', 'test_metadata_set', Country='Sweden', Language='Urdu')

        self.assertEqual(response['Id'], '56e62c95-2cfa-4c61-2997-08d5fb8f513a')

    def test_metadata_get(self):
        ns = 'system.integrationtest'
        key = 'test_metadata_get.76cde00e-1224-47ac-9095-b3f1450c476d'
        response = self.client.metadata_set(
            ns, key, Country='India', Language='Hindi')

        response = self.client.metadata_get(namespace=ns, key=key)

        self.assertEqual(response['Namespace'], ns)
        self.assertEqual(response['Key'], key)
        self.assertEqual(response['Value']['Country'], 'India')
        self.assertEqual(response['Value']['Language'], 'Hindi')

    def test_metadata_browse(self):
        response = self.client.metadata_browse()

        self.assertEqual(len([m for m in response if m == 'system.reservoir']), 1)

    def test_metadata_browse_with_namespace(self):
        ns = 'system.integrationtest'
        key = 'test_metadata_set'
        response = self.client.metadata_browse(namespace=ns)

        self.assertEqual(len([m for m in response if m == key]), 1)

    def test_metadata_browse_with_namespace_and_key(self):
        ns = 'system.integrationtest'
        key = 'test_metadata_browse_with_namespace_and_key.04f43138-2f52-4038-bcdd-e0fbbd16d874'
        response = self.client.metadata_set(
            ns, key, Country='Norway', Language='Norwegian')

        response = self.client.metadata_browse(namespace=ns, key=key)

        print response
        self.assertEqual(len([m for m in response if m == 'Country']), 1)


if __name__ == '__main__':
    unittest.main()
