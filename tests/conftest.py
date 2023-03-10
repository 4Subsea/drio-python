from io import BytesIO
from pathlib import Path
from unittest.mock import call

import pytest
import requests

TEST_PATH = Path(__file__).parent


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
}


@pytest.fixture(autouse=True)
def mock_requests(monkeypatch):
    """Patch requests.sessions.Session.request for all tests."""
    monkeypatch.setattr("requests.sessions.Session.request", response_factory)


def response_factory(_, method, url, *args, **kwargs):
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

    response.mock_calls = call(method, url, *args, **kwargs)

    return response
