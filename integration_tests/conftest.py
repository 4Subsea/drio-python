import os

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
            series_created.add(return_value["TimeSeriesId"])
            return return_value

    monkeypatch.setattr("datareservoirio.Client", ClientStoreCreated)


@pytest.fixture
def auth_session():
    client_id = os.getenv("DRIO_CLIENT_ID")
    client_secret = os.getenv("DRIO_CLIENT_SECRET")
    return drio.authenticate.ClientAuthenticator(client_id, client_secret)


@pytest.fixture
def client(auth_session, store_created_series, series_created):
    client = drio.Client(auth_session)

    yield client

    # Delete all created timeseries from DataReservoir.io (if not already deleted)
    while series_created:
        series_id_i = series_created.pop()
        try:
            client.delete(series_id_i)
        except HTTPError:
            pass
