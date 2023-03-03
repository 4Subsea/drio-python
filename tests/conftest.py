import json
from pathlib import Path

import pytest
import requests

TEST_PATH = Path(__file__).parent


URI_CONTENT_MAP = {
    "example/drio/blob/file": str(TEST_PATH / "testdata" / "example_drio_blob_file.csv"),
    "example/drio/api/output": str(TEST_PATH / "testdata" / "example_drio_api_output.json"),
}


def url_to_file(url):
    if url not in URI_CONTENT_MAP:
        raise ValueError
    return URI_CONTENT_MAP[url]


class MockResponse:
    def __init__(self, content_path=None, stream=False, **kwargs):
        self._content_path = content_path
        self._stream = stream
        self._raise_for_status = False

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
    def mock_get(url, **kwargs):
        file_path = url_to_file(url)
        return MockResponse(content_path=file_path, **kwargs)

    monkeypatch.setattr(requests, "get", mock_get)
