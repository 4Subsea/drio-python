import logging
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from response_cases import RESPONSE_CASES, RESPONSE_CASES_DEFAULT, RESPONSE_CASES_GENERAL

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
            "default": RESPONSE_CASES_DEFAULT.copy(),
            "general": RESPONSE_CASES_GENERAL.copy(),
        }

        def __init__(self):
            self._response_cases = self._CASES["default"]

        def set(self, label):
            self._response_cases = self._CASES[label]

        def __getitem__(self, key):
            return self._response_cases[key]

    handler = ResponseCaseHandler()

    return handler


@pytest.fixture(autouse=True)
def mock_requests(monkeypatch, response_cases):
    """Patch requests.sessions.Session.request for all tests."""
    mock = Mock(wraps=ResponseFactory(response_cases))
    monkeypatch.setattr("requests.sessions.Session.request", mock)
    return mock


class ResponseFactory:
    def __init__(self, response_cases):
        self._response_cases = response_cases

    def __call__(self, method, url, *args, **kwargs):
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
            raise ValueError(f"Unrecognized URL: {url}")
        else:
            spec.update({"url": url, "raw": BytesIO(spec.pop("_content", None))})

        response = requests.Response()

        for attr, value in spec.items():
            setattr(response, attr, value)

        return response


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
        spec = RESPONSE_CASES[(method.upper(), url)].copy()
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
