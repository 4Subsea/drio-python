import logging
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

TEST_PATH = Path(__file__).parent


@pytest.fixture(autouse=True)
def disable_logging():
    """Disable logging to Application Insight"""
    logger = logging.getLogger("datareservoirio.client_metric_appinsight")
    disabled = logger.disabled
    logger.disabled = True
    yield None
    logger.disabled = disabled


"""
RESPONSE_CASES defines attributes assigned to ``requests.Response`` object. The
following attributes can be used as needed:

    * ``_content``
    * ``status_code``
    * ``headers``
    * ``history``
    * ``encoding``
    * ``reason``
    * ``cookies``
    * ``elapsed``
    * ``request``

Note that ``url`` is defined as part of the key in RESPONSE_CASES.
See ``requests.Response`` source code for more details.
"""
RESPONSE_CASES = {
    # description: blob (numeric) from remote storage
    ("GET", "http://example/drio/blob/file"): {
        "_content": (
            TEST_PATH / "testdata" / "example_drio_blob_file.csv"
        ).read_bytes(),
        "status_code": 200,
        "reason": "OK",
    },
    # description: blob do not exist in remote storage
    ("GET", "http://example/no/exist"): {
        "status_code": 404,
        "reason": "Not Found",
    },
    # description: put data to blob
    ("PUT", "http://example/blob/url"): {
        "status_code": 201,
        "reason": "Created",
    },
}


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
