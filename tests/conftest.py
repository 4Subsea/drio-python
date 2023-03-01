import pytest
from unittest.mock import Mock
import json

import requests

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
def request_response2():
    class ResponseFactory(Mock):
        def __init__(self):
            self._raise_for_status = False
            self._content_path = None
            super().__init__()

        def raise_for_status(self):
            if self._raise_for_status:
                raise requests.HTTPError()
            return None

        @property
        def content(self):
            with open(self._content_path, model="rb") as f:
                content = f.read()
            return content

        def json(self, **kwargs):
            return json.loads(self.content, **kwargs)

    return ResponseFactory()
