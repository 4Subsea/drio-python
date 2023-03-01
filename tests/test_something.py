import pytest
from unittest.mock import Mock
from pathlib import Path
import datareservoirio as drio


TEST_PATH = Path(__file__).parent


def test_something():
    """
    Temporary test so that tox runs without errors while waiting for actual tests
    to be added.

    TODO: Remove when actual tests are added.
    """
    assert 1 == 1


class Test__blob_to_df:

    @pytest.fixture
    def mock_response_get(self, monkeypatch, get_response):

        get_response._content_path = TEST_PATH / "testdata" / "example_drio_blob_file.csv"

        mock_get = Mock()
        mock_get.return_value = get_response

        monkeypatch.setattr("datareservoirio.storage.storage.requests.get", mock_get)

    def test__blob_to_df(self, mock_response_get):
        df = drio.storage.storage._blob_to_df("some/url")
