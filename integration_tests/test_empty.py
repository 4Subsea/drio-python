import os

import pandas as pd
import pytest
from requests import HTTPError

import datareservoirio as drio


def test_empty(cleanup_series):
    """
    Integration test for creating/appending/deleting timeseries with numeric values
    and datetime index.

    Tests the following:
        * Create a timeseries in DataReservoir.io.
        * Append more data to the created timeseries.
        * Delete the timeseries from DataReservoir.io

    """

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(auth_session)

    # Create and upload timeseries to DataReservoir.io
    response_create = client.create(series=None, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]
    cleanup_series.add(series_id)

    # Get data from DataReservoir.io
    series_full_out = client.get(series_id, start=None, end=None)
    series_partial_out = client.get(
        series_id, start="2022-01-01 00:00", end="2022-01-02 00:00"
    )

    # Check downloaded data
    series_empty = pd.Series(
        index=pd.DatetimeIndex([], tz="utc"), dtype="object", name="values"
    )
    pd.testing.assert_series_equal(series_full_out, series_empty, check_freq=False)
    pd.testing.assert_series_equal(series_partial_out, series_empty, check_freq=False)

    # Delete timeseries from DataReservoir.io
    client.delete(series_id)

    # Check that the timeseries is deleted
    with pytest.raises(HTTPError):
        _ = client.get(series_id)
