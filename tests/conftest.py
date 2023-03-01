import glob
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

TEST_PATH = Path(__file__).parent


@pytest.fixture
def request_response():
    response = Mock()
    response._raise_for_status = False
    response._content_path = "testdata/content.json"

    def raise_for_status_side_effect(*args, **kwargs):
        if response._raise_for_status:
            raise requests.HTTPError()
        return None

    response.raise_for_status.side_effect = raise_for_status_side_effect

    response.content = None


@pytest.fixture
def get_response():
    class ResponseFactory:
        def __init__(self):
            self._stream = False
            self._raise_for_status = False
            self._content_path = None

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

    return ResponseFactory()


@pytest.fixture
def mock_requests_get(monkeypatch, get_response):
    def mock_get(url, *args, **kwargs):
        path = TEST_PATH / "testdata" / url.replace("/", "_")
        path_matches = glob.glob(str(path) + "*")
        if len(path_matches) > 1:
            raise ValueError()

        get_response._content_path = path_matches[0]

        return get_response

    monkeypatch.setattr(requests, "get", mock_get)
