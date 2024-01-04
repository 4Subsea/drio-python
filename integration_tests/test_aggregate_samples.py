import os

import pandas as pd
import pytest
from requests import HTTPError

import datareservoirio as drio


def test_empty(cleanup_series):
    """
    Integration test for the samples/aggregate endpoint.

    Tests the following:
        * Creates an empty timeseries in DataReservoir.io.
        * Fetches it using the new endpoint.
    """

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(auth_session, cache=False)

    # Create and upload timeseries to DataReservoir.io
    response_create = client.create(series=None, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]

    # For cleaning up after test runs
    cleanup_series.add(series_id)

    # Get data from DataReservoir.io using the samples/aggregate endpoint
    series = client.get_samples_aggregate(series_id)

    # Check downloaded data
    series_expected = pd.Series(
        index=pd.DatetimeIndex([], tz="utc"), dtype="object", name="values"
    )
    pd.testing.assert_series_equal(series, series_expected, check_freq=False)

