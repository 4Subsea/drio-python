import os
import time
from pathlib import Path

import pandas as pd
import pytest

import datareservoirio as drio
from datareservoirio._utils import DataHandler

TEST_PATH = Path(__file__).parent


class Test_Client:
    """
    Tests the ``datareservoirio.Client`` class.
    """

    @pytest.fixture
    def group1_data(self):
        data = DataHandler.from_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "dataframe.csv"
        )
        return data

    @pytest.fixture
    def group2_data(self):
        """Overlapping data"""
        data = DataHandler.from_csv(
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "dataframe.csv"
        )
        return data

    @pytest.fixture
    def client(self, auth_session):
        return drio.Client(auth_session, cache=False)

    @pytest.fixture
    def cache_root(self, tmp_path):
        return tmp_path / ".cache"

    @pytest.fixture
    def client_with_cache(self, auth_session, cache_root):
        cache_opt = {"max_size": 1024, "cache_root": cache_root}
        return drio.Client(auth_session, cache=True, cache_opt=cache_opt)

    def test__init__(self, auth_session, tmp_path):
        cache_opt = {
            "max_size": 1024,
            "cache_root": tmp_path / ".cache",
        }
        drio.Client(auth_session, cache=True, cache_opt=cache_opt)

    @pytest.mark.parametrize(
        "start, end",
        [
            (1672358400000000000, 1672703939999999999 + 1),
            ("2022-12-30T00:00:00+00:00", "2023-01-02T23:59:00+00:00"),
            (pd.to_datetime("2022-12-30T00:00"), pd.to_datetime("2023-01-02T23:59:00")),
            (None, None),
        ],
    )
    def test_get(self, mock_requests, client, start, end, group1_data):
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=start,
            end=end,
            convert_date=False,
        )

        series_expect = group1_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)
        # Check that the correct HTTP request is made
        request_url_expect = "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672703939999999999"
        mock_requests.call_args_list[0].kwargs["url"] = request_url_expect

    def test_get_convert_date(self, client, group1_data):
        series_out = client.get(
            "2fee7f8a-664a-41c9-9b71-25090517c275",
            start=1672358400000000000,
            end=1672703939999999999 + 1,
            convert_date=True,
        )

        series_expect = group1_data.as_series()
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_raise_empty(self, client):
        with pytest.raises(ValueError):
            client.get("e3d82cda-4737-4af9-8d17-d9dfda8703d0", raise_empty=True)

    def test_get_raises_end_not_after_start(self, client):
        start = 1672358400000000000
        end = start - 1
        with pytest.raises(ValueError):
            client.get(
                "2fee7f8a-664a-41c9-9b71-25090517c275",
                start=start,
                end=end,
            )

    def test_get_empty(self, client):
        series_out = client.get(
            "e3d82cda-4737-4af9-8d17-d9dfda8703d0",
            start=None,
            end=None,
            convert_date=True,
        )

        series_expect = pd.Series(name="values", dtype="object")
        series_expect.index.name = None
        series_expect.index = pd.to_datetime(series_expect.index, utc=True)

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_overlapping(self, client, group2_data):
        series_out = client.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )

        series_expect = group2_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)

    def test_get_with_cache(
        self, client_with_cache, cache_root, STOREFORMATVERSION, group2_data
    ):
        # Check that the cache folder is empty
        cache_path_expect = cache_root / STOREFORMATVERSION
        assert cache_path_expect.exists()
        assert len(list(cache_path_expect.iterdir())) == 0

        # Get data (and store in cache)
        series_out = client_with_cache.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )

        series_expect = group2_data.as_series()
        pd.testing.assert_series_equal(series_out, series_expect)

        # Check that the cache folder now contains six files
        assert len(list(cache_path_expect.iterdir())) == 6

        # Get data (from cache)
        time_before_get = time.time()
        time.sleep(0.1)
        series_out = client_with_cache.get(
            "693cb0b2-3599-46d3-b263-ea913a648535",
            start=1672358400000000000,
            end=1672617600000000000 + 1,
            convert_date=False,
        )
        time.sleep(0.1)
        time_after_get = time.time()
        for cache_file_i in cache_path_expect.iterdir():
            time_access_file_i = os.path.getatime(cache_file_i)  # last access time
            assert time_before_get < time_access_file_i < time_after_get

        pd.testing.assert_series_equal(series_out, series_expect)

    def test_ping(self, client):
        ping_out = client.ping()

        ping_expect = {
            "Time": "2023-04-27T11:50:09.3849453+00:00",
            "Status": "pong",
            "BackendStatus": {
                "Status": "pong-from-backend",
                "OperationId": "956ae5486179f34abadb3188d87c11c8",
                "NodeName": "_front003_4",
                "NodeId": "b9f95b31b17f9893ff7c9650fcfae710",
                "NodeInstanceId": "133268133774998761",
                "NodeType": "front003",
            },
            "AuthScheme": "Bearer",
            "Claims": [
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress:vrs@4subsea.com",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname:Vegard",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname:Rørvik Solum",
                "name:Vegard Rørvik Solum",
                "http://schemas.microsoft.com/identity/claims/identityprovider:4subsea-prod-ip",
                "organizationId:2c4ee562-6261-4018-a1b1-8837ab526944",
                "nameidentifier:4f65c57c-0687-4c3f-9e78-9d45d552f6b5",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier:d2de7042-8074-4031-b9a4-00dd324d0553",
                "externalId:4subsea-prod-ip|d2de7042-8074-4031-b9a4-00dd324d0553",
                "nonce:638149222540438885.N2JlNDA5OGYtMjk0Ny00OTYxLWEzMjMtNjEzOWYxM2Y4NDg3ZDdjZGUzMzMtZjY5OS00YzM2LTllMGEtNGY5NTI5YTZjOGYw",
                "http://schemas.microsoft.com/identity/claims/scope:write read",
                "azp:6b879622-4c52-43a3-ba23-2e9595dd996b",
                "ver:1.0",
                "iat:1682596006",
                "aud:ff4737b5-3602-46a0-9805-bd18314700c1",
                "exp:1682599606",
                "iss:https://4subseaid.b2clogin.com/c8ea118f-bd50-422e-8503-1d8055a3dcf0/v2.0/",
                "nbf:1682596006",
                "http://schema.4subsea.net/userId:b52056b9-174e-46d9-a446-d2e33a389a41",
                "http://schema.4subsea.net/organizationId:2c4ee562-6261-4018-a1b1-8837ab526944",
            ],
        }

        assert ping_out == ping_expect
