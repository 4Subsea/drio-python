import unittest

import requests

import datareservoirio
from datareservoirio.authenticate import ClientAuthenticator
from tests_integration._auth import CLIENT


class Test_ClientMetadata(unittest.TestCase):
    def setUp(self):
        self.auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        self.client = datareservoirio.Client(self.auth, cache=False)

    def tearDown(self):
        self.auth.close()

    def test_metadata_set_with_data_returns_id_of_existing_metadata(self):
        response = self.client.metadata_set(
            "system.integrationtest",
            "test_metadata_set",
            Country="Sweden",
            Language="Urdu",
        )

        self.assertEqual(response["Id"], "56e62c95-2cfa-4c61-2997-08d5fb8f513a")

    def test_set_metadata_with_overwrite(self):
        ts_id = "6f64c5a8-bd28-4ca8-9df5-ffed0a0259b4"
        ns = "system.integrationtest"
        key = "test_set_metadata.{}".format(ts_id)
        self.client._timeseries_api.create(ts_id)
        self.client._metadata_api.put(namespace=ns, key=key, overwrite=True, Data=42)

        self.client.set_metadata(ts_id, namespace=ns, key=key, overwrite=True, Data=37)

        response = self.client.metadata_get(namespace=ns, key=key)
        self.assertEqual(response["Value"]["Data"], 37)

    def test_set_metadata_without_overwrite_throws(self):
        ts_id = "3c0b0936-1325-4f2b-a32c-d4b1ac80f4f8"
        ns = "system.integrationtest"
        key = "test_set_metadata.{}".format(ts_id)
        self.client._timeseries_api.create(ts_id)
        self.client._metadata_api.put(namespace=ns, key=key, overwrite=True, Data=101)

        with self.assertRaises(ValueError):
            self.client.set_metadata(ts_id, namespace=ns, key=key, Data=37)

        response = self.client.metadata_get(namespace=ns, key=key)
        self.assertEqual(response["Value"]["Data"], 101)

    def test_metadata_get(self):
        ns = "system.integrationtest"
        key = "test_metadata_get.76cde00e-1224-47ac-9095-b3f1450c476d"
        response = self.client.metadata_set(ns, key, Country="India", Language="Hindi")

        response = self.client.metadata_get(namespace=ns, key=key)

        self.assertEqual(response["Namespace"], ns)
        self.assertEqual(response["Key"], key)
        self.assertEqual(response["Value"]["Country"], "India")
        self.assertEqual(response["Value"]["Language"], "Hindi")

    def test_metadata_browse(self):
        response = self.client.metadata_browse()

        self.assertEqual(len([m for m in response if m == "system.reservoir"]), 1)

    def test_metadata_browse_with_namespace(self):
        ns = "system.integrationtest"
        key = "test_metadata_set"
        response = self.client.metadata_browse(namespace=ns)

        self.assertEqual(len([m for m in response if m == key]), 1)

    def test_metadata_browse_with_namespace_and_key(self):
        ns = "system.integrationtest"
        key = "test_metadata_browse_with_namespace_and_key.04f43138-2f52-4038-bcdd-e0fbbd16d874"
        response = self.client.metadata_set(
            ns, key, Country="Norway", Language="Norwegian"
        )

        response = self.client.metadata_browse(namespace=ns, key=key)

        self.assertEqual(len([m for m in response if m == "Country"]), 1)

    def test_metadata_delete_with_id(self):
        ns = "system.integrationtest"
        key = "test_metadata_get.319f8f14-5872-4862-950c-2bbacba54939"
        response = self.client.metadata_set(
            ns, key, Country="Norway", Language="Norwegian"
        )
        mid = response["Id"]

        self.client.metadata_delete(mid)

        with self.assertRaises(requests.exceptions.HTTPError) as context:
            self.client.metadata_get(metadata_id=mid)
        self.assertEqual(context.exception.response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
