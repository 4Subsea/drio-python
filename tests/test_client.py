from pathlib import Path
import pytest
import pandas as pd
import datareservoirio as drio


TEST_PATH = Path(__file__).parent


class Test_Client:
    """
    Tests the ``datareservoirio.Client`` class.
    """
    @pytest.fixture
    def client(self, auth_session):
        return drio.Client(auth_session, cache=False)

    def test__init__(self, auth_session):
        client = drio.Client(auth_session, cache=False)

        assert client._auth_session is auth_session
        assert isinstance(client._timeseries_api, drio.rest_api.TimeSeriesAPI)
        assert isinstance(client._files_api, drio.rest_api.FilesAPI)
        assert isinstance(client._metadata_api, drio.rest_api.MetadataAPI)
        assert isinstance(client._storage, drio.storage.Storage)

    def test_get(self, client):
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=1672358400000000000,
            end=1672703940000000000
        )

        df_expect = pd.read_csv(
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv",
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "float64"},
            encoding="utf-8",
        )

        pd.testing.assert_frame_equal(series_out, df_expect["values"])
