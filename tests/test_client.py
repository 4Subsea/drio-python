import pytest
import datareservoirio as drio


class Test_Client:
    """
    Tests the ``datareservoirio.Client`` class.
    """
    @pytest.fixture
    def client_no_cache(self, auth_session):
        return drio.Client(auth_session, cache=False)

    def test__init__(self, auth_session):
        client = drio.Client(auth_session, cache=False)

        assert client._auth_session is auth_session
        assert isinstance(client._timeseries_api, drio.rest_api.TimeSeriesAPI)
        assert isinstance(client._files_api, drio.rest_api.FilesAPI)
        assert isinstance(client._metadata_api, drio.rest_api.MetadataAPI)
        assert isinstance(client._storage, drio.storage.Storage)

    def test_get(self, client_no_cache):
        pass
