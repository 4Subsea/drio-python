import pytest
from requests import HTTPError
import datareservoirio as drio


@pytest.fixture
def series_created():
    """List of created timeseries IDs"""
    return set()


@pytest.fixture
def store_created_series(monkeypatch, series_created):
    """Store created timeseries IDs to the ``series_created`` list"""
    class ClientStoreCreated(drio.Client):
        def create(self, *args, **kwargs):
            return_value = super().create(*args, **kwargs)
            series_id = return_value["TimeSeriesId"]
            series_created.add(series_id)
            return return_value

    monkeypatch.setattr("datareservoirio.Client", ClientStoreCreated)


@pytest.fixture
def auth_session():
    client_id = "0ee5d1b4-2271-4595-8c23-f2361d713a47"
    client_secret = "2iF8Q~t_WniDYJmFnM8cjB6KoBGH-2C9Imij9cKa"
    return drio.authenticate.ClientAuthenticator(client_id, client_secret)


@pytest.fixture
def client(auth_session, store_created_series, series_created):

    client = drio.Client(auth_session)

    yield client

    # Delete all created timeseries from DataReservoir.io
    while series_created:
        series_id_i = series_created.pop()
        try:
            client.delete(series_id_i)
        except HTTPError:
            pass
