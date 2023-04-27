import logging
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from response_cases import RESPONSE_CASES

from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


@pytest.fixture(autouse=True)
def disable_logging(monkeypatch):
    """Disable logging to Application Insight"""
    monkeypatch.setattr("datareservoirio.client.AzureLogHandler", logging.NullHandler())


@pytest.fixture(autouse=True)
def mock_requests(monkeypatch):
    """Patch requests.sessions.Session.request for all tests."""
    mock = Mock(wraps=response_factory)
    monkeypatch.setattr("requests.sessions.Session.request", mock)
    return mock


def response_factory(method, url, **kwargs):
    """
    Generate response based on request call and lookup in ``RESPONSE_CASES``.
    Attributes assigned to ``requests.Response`` object.

    Notes
    -----
    The first argument, ``_`` will be an instance of ``requests.sessions.Session``
    since ``self`` passed to the ``request`` method. See ``mock_requests``.
    """
    try:
        spec = RESPONSE_CASES[(method.upper(), url)].copy()
    except KeyError:
        raise ValueError(f"Unrecognized URL: {url}")
    else:
        spec.update({"url": url, "raw": BytesIO(spec.pop("_content", None))})

    response = requests.Response()

    for attr, value in spec.items():
        setattr(response, attr, value)

    # Create the Request.
    req = requests.Request(
        method=method.upper(),
        url=url,
        headers=kwargs.get("headers"),
        files=kwargs.get("files"),
        data=kwargs.get("data") or {},
        json=kwargs.get("json"),
        params=kwargs.get("params") or {},
        auth=kwargs.get("auth"),
        cookies=kwargs.get("cookies"),
        hooks=kwargs.get("hooks"),
    )
    response.request = req.prepare()

    return response


@pytest.fixture()
def bytesio_with_memory(monkeypatch):
    class BytesIOMemory(BytesIO):
        def close(self, *args, **kwargs):
            self.memory = self.getvalue()
            super().close(*args, **kwargs)

    monkeypatch.setattr("io.BytesIO", BytesIOMemory)


@pytest.fixture
def auth_session():
    return requests.Session()


@pytest.fixture
def data_float():
    """Data with float values"""
    index_list = (
        1640995215379000000,
        1640995219176000000,
        1640995227270000000,
        1640995267223000000,
        1640995271472000000,
    )

    values_list = (-0.2, -0.1, 0.2, 0.1, 1.2)

    series = pd.Series(data=values_list, index=index_list, name="values")

    data_handler = DataHandler(series)

    return data_handler


@pytest.fixture
def data_string():
    """Data with string values"""
    index_list = (
        1640995215379000000,
        1640995219176000000,
        1640995227270000000,
        1640995267223000000,
        1640995271472000000,
    )

    values_list = ("foo", "bar", "baz", "foobar", "abcd")

    series = pd.Series(data=values_list, index=index_list, name="values")

    data_handler = DataHandler(series)

    return data_handler


@pytest.fixture
def STOREFORMATVERSION():
    return "v3"
