import os

import pytest
from requests import HTTPError

import datareservoirio as drio


@pytest.fixture(autouse=True, scope="session")
def set_environment_qa():
    """Set environment to 'QA'"""
    env = drio.environments.Environment()
    env.set_qa()


@pytest.fixture
def store_created_series(monkeypatch):
    """Store created timeseries IDs"""

    class ClientStoreCreated(drio.Client):
        _series_created = set()

        def create(self, *args, **kwargs):
            return_value = super().create(*args, **kwargs)
            self._series_created.add(return_value["TimeSeriesId"])
            return return_value

    monkeypatch.setattr("datareservoirio.Client", ClientStoreCreated)


@pytest.fixture
def auth_session():
    client_id = os.getenv("DRIO_CLIENT_ID")
    client_secret = os.getenv("DRIO_CLIENT_SECRET")
    return drio.authenticate.ClientAuthenticator(client_id, client_secret)


@pytest.fixture
def client(auth_session, store_created_series):
    client = drio.Client(auth_session)

    yield client

    # Delete all created timeseries from DataReservoir.io (if not already deleted)
    while client._series_created:
        series_id_i = client._series_created.pop()
        try:
            client.delete(series_id_i)
        except HTTPError:
            pass
