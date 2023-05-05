import logging
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
import response_cases as rc

from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


@pytest.fixture(autouse=True)
def disable_logging(monkeypatch):
    """Disable logging to Application Insight"""
    monkeypatch.setattr("datareservoirio.client.AzureLogHandler", logging.NullHandler())


@pytest.fixture
def response_cases():
    class ResponseCaseHandler:
        _CASES = {
            "general": rc.GENERAL.copy(),
            "azure-blob-storage": rc.AZURE_BLOB_STORAGE.copy(),
            "datareservoirio-api": rc.DATARESERVOIRIO_API.copy(),
            "group1": rc.GROUP1.copy(),
            "group2": rc.GROUP2.copy(),
            "group3": rc.GROUP3.copy(),
            "group3-failed": rc.GROUP3_FAILED.copy(),
            "group3-upload-raises": rc.GROUP3_UPLOAD_RAISES.copy(),
            "group4": rc.GROUP4.copy(),
        }

        def __init__(self):
            self.set("general")

        def set(self, label):
            if label not in self._CASES:
                raise ValueError("Unknown response label")
            self._label = label

        def __getitem__(self, key):
            return self._CASES[self._label][key]

    handler = ResponseCaseHandler()
    return handler


@pytest.fixture(autouse=True)
def mock_requests(monkeypatch, response_cases):
    """Patch requests.sessions.Session.request for all tests."""
    mock = Mock(wraps=ResponseFactory(response_cases))
    monkeypatch.setattr("requests.sessions.Session.request", mock)
    return mock


class ResponseFactory:
    """
    Response factory.

    Parameters
    ----------
    response_cases : dict-like
        Dictionary with (METHOD, URL) as key.
    """

    def __init__(self, response_cases):
        self._response_cases = response_cases

    def __call__(self, method, url, **kwargs):
        """
        Generate response based on request call and lookup in ``RESPONSE_CASES``.
        Attributes assigned to ``requests.Response`` object.

        Notes
        -----
        The first argument, ``_`` will be an instance of ``requests.sessions.Session``
        since ``self`` passed to the ``request`` method. See ``mock_requests``.
        """
        try:
            spec = self._response_cases[(method.upper(), url)].copy()
        except KeyError:
            raise ValueError(f"Unrecognized METHOD + URL: {method.upper()} {url}")
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
