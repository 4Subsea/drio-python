import json
from pathlib import Path

from unittest.mock import MagicMock, PropertyMock

import pytest
import requests

TEST_PATH = Path(__file__).parent


URI_RESPONSE_MAP = {
    "example/drio/blob/file": {
        "content_path": str(TEST_PATH / "testdata" / "example_drio_blob_file.csv"),
        "raise_for_status": False,
    },
    "example/no/exist": {
        "content_path": None,
        "raise_for_status": True,
    },
}


def uri_to_config(uri):
    """
    Get response mock configuration from a URI.

    Parameters
    ----------
    uri : str
        URI identifying a response.
    """
    try:
        config = URI_RESPONSE_MAP[uri]
    except KeyError:
        raise ValueError(f"Unrecognized URL: {uri}")
    return config


class MockGetResponse:
    """
    Mocks a :class:`requests.Response` object.

    Configure the mock by setting appropriate values for the configuration parameters
    given during initialization.

    Paramters
    ---------
    content_path : str, optional
        Path to file with the response content.
    raise_for_status : bool
        Weather to raise a :class:`requests.HTTPError` when the :meth:`raise_for_status`
        method is called.
    **kwargs :
        Optional keyword arguments passed to the `get` method (which returns the response).
    """
    def __init__(
        self,
        content_path=None,
        raise_for_status=False,
        **kwargs,
    ):
        self._content_path = content_path
        self._raise_for_status = raise_for_status
        self._stream = kwargs.get("stream", False)

    def raise_for_status(self):
        if self._raise_for_status:
            raise requests.HTTPError()
        return None

    @property
    def content(self):
        with open(self._content_path, mode="rb") as f:
            content = f.read()
        return content

    def iter_content(self, chunk_size=1, decode_unicode=False):
        if not self._stream:
            chunk_size = -1  # read all data

        with open(self._content_path, mode="rb") as f:
            while content_i := f.read(chunk_size):
                if decode_unicode:
                    yield content_i.decode()
                yield content_i

    def json(self, **kwargs):
        return json.loads(self.content, **kwargs)


@pytest.fixture
def mock_requests_get(monkeypatch):
    """
    Mocks the :func:`requests.get` function.

    The response configuration is determined by the endpoint url (and possibly other
    keyword arguments) passed to :func:`requests.get`.
    """
    def mock_get(url, **kwargs):
        config = uri_to_config(url)
        return MockGetResponse(**config, **kwargs)

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def response():
    response = MagicMock(spec=requests.models.Response)

    # Config
    response._config = {
        "content_path": None,
        "raise_for_status": False,
        "stream": False,
    }

    def raise_for_status_side_effect(*args, **kwargs):
        if response._config["raise_for_status"]:
            raise requests.HTTPError

    response.raise_for_status.side_effect = raise_for_status_side_effect

    def content_side_effect(*args, **kwargs):
        with open(response._config["content_path"], mode="rb") as f:
            content = f.read()
        return content

    type(response).content = PropertyMock(side_effect=content_side_effect)

    def iter_content_side_effect(chunk_size=1, decode_unicode=False):
        if not response._config["stream"]:
            chunk_size = -1  # read all data

        with open(response._config["content_path"], mode="rb") as f:
            while content_i := f.read(chunk_size):
                if decode_unicode:
                    yield content_i.decode()
                yield content_i

    response.iter_content.side_effect = iter_content_side_effect

    def json_side_effect(**kwargs):
        return json.loads(response.content, **kwargs)

    response.json.side_effect = json_side_effect

    return response


@pytest.fixture
def mock_requests_get2(monkeypatch, response):
    def mock_get(url, **kwargs):
        config = uri_to_config(url)

        response._config.update(config)

        return response

    monkeypatch.setattr(requests, "get", mock_get)
