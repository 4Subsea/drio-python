import os
import random
import string

import pandas as pd
import pytest
from requests import HTTPError

import datareservoirio as drio


def test_string(cleanup_series):
    """
    Integration test for creating/appending/deleting timeseries with text/string values
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

    # Create some dummy data
    start_a = "2022-12-30 00:00"
    end_a = "2023-01-02 00:00"
    freq_a = pd.to_timedelta(0.1, "s")
    index_a = pd.date_range(start_a, end_a, freq=freq_a, tz="utc", inclusive="left")
    values_a = [
        "".join(random.choices(string.ascii_lowercase, k=5))
        for _ in range(len(index_a))
    ]
    series_a = pd.Series(data=values_a, index=index_a, name="values")

    start_b = "2023-01-02 00:00"
    end_b = "2023-01-02 03:00"
    freq_b = pd.to_timedelta(0.1, "s")
    index_b = pd.date_range(start_b, end_b, freq=freq_b, tz="utc", inclusive="left")
    values_b = [
        "".join(random.choices(string.ascii_lowercase, k=5))
        for _ in range(len(index_b))
    ]
    series_b = pd.Series(data=values_b, index=index_b, name="values")

    series_a_and_b = pd.concat([series_a, series_b])

    # Create and upload timeseries to DataReservoir.io
    response_create = client.create(series=series_a, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]
    cleanup_series.add(series_id)

    # Get data from DataReservoir.io (before data is appended)
    series_full_before_append = client.get(series_id, start=None, end=None)

    # Check downloaded data
    pd.testing.assert_series_equal(
        series_a, series_full_before_append, check_freq=False
    )

    # Append more data to the timeseries
    _ = client.append(series_b, series_id, wait_on_verification=True)

    # Get data from DataReservoir.io (after data is appended)
    series_full_after_append = client.get(series_id, start=None, end=None)

    # Check downloaded data
    pd.testing.assert_series_equal(
        series_a_and_b, series_full_after_append, check_freq=False
    )

    # Get data between two dates from DataReservoir.io
    start = pd.to_datetime("2023-01-01 00:00", utc=True)
    end = pd.to_datetime("2023-01-01 03:00", utc=True)
    delta = pd.to_timedelta(1, "ns")
    series_partial = client.get(series_id, start=start, end=end)

    # Check downloaded data
    pd.testing.assert_series_equal(
        series_a_and_b.loc[start : end - delta], series_partial, check_freq=False
    )

    # Delete timeseries from DataReservoir.io
    client.delete(series_id)

    # Check that the timeseries is deleted
    with pytest.raises(HTTPError):
        _ = client.get(series_id)
