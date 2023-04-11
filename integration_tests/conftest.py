import os

import pytest
from requests import HTTPError

import datareservoirio as drio


@pytest.fixture(autouse=True, scope="session")
def set_environment_qa():
    """Set environment to 'QA'"""
    env = drio.environments.Environment()
    env.set_qa()


@pytest.fixture(scope="session")
def auth_session():
    client_id = os.getenv("DRIO_CLIENT_ID")
    client_secret = os.getenv("DRIO_CLIENT_SECRET")
    auth_session = drio.authenticate.ClientAuthenticator(client_id, client_secret)
    return auth_session


@pytest.fixture()
def client(auth_session):
    return drio.Client(auth_session, cache=False)


@pytest.fixture()
def cleanup_series(client):
    series_created = set()

    yield series_created

    # Delete all created timeseries from DataReservoir.io (if not already deleted)
    while series_created:
        series_id_i = series_created.pop()
        try:
            client.delete(series_id_i)
        except HTTPError:
            pass
