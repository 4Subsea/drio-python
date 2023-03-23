import pytest
from requests import HTTPError
import datareservoirio as drio


@pytest.fixture
def series_created():
    return set()


@pytest.fixture
def store_created(monkeypatch, series_created):

    class ClientStoreCreated(drio.Client):
        def create(self, *args, **kwargs):
            return_value = super().create(*args, **kwargs)
            series_id = kwargs.get("series_id") or args[0]
            series_created.add(series_id)
            return return_value

    monkeypatch.setattr("datareservoirio.Client", ClientStoreCreated)


@pytest.fixture
def client(store_created, series_created):
    client_id = "0ee5d1b4-2271-4595-8c23-f2361d713a47"
    client_secret = "2iF8Q~t_WniDYJmFnM8cjB6KoBGH-2C9Imij9cKa"
    client = drio.authenticate.ClientAuthenticator(client_id, client_secret)
    yield client

    for series_id_i in series_created:
        try:
            client.delete(series_id_i)
        except HTTPError:
            pass
        finally:
            series_created.remove(series_id_i)
