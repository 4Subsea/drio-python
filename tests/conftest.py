import logging
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
from response_cases import RESPONSE_CASES

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


def response_factory(method, url, *args, **kwargs):
    """
    Generate response based on request call and lookup in ``RESPONSE_CASES``.
    Attributes assigned to ``requests.Response`` object.

    Notes
    -----
    The first argument, ``_`` will be an instance of ``requests.sessions.Session``
    since ``self`` passed to the ``request`` method. See ``mock_requests``.
    """
    try:
        spec = RESPONSE_CASES[(method.upper(), url)]
    except KeyError:
        raise ValueError(f"Unrecognized URL: {url}")
    else:
        spec.update({"url": url, "raw": BytesIO(spec.pop("_content", None))})

    response = requests.Response()

    for attr, value in spec.items():
        setattr(response, attr, value)

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
