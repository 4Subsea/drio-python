"""
Integration test for creating/appending/deleting timeseries with numeric values
and datetime index.

Tests the following:
    * Create a timeseries in DataReservoir.io.
    * Append more data to the created timeseries.
    * Delete the timeseries from DataReservoir.io

"""
import numpy as np
import pandas as pd

import datareservoirio as drio


def test_numeric_datetime(client):
    # Set environment to 'TEST'
    env = drio.environments.Environment()
    env.set_test()

    # Create some dummy data
    start_a = "2022-12-28 00:00"
    end_a = "2023-01-02 00:00"
    freq_a = pd.to_timedelta(0.1, "s")
    timestamps_a = pd.date_range(
        start_a, end_a, freq=freq_a, tz="utc", inclusive="left"
    )
    values_a = np.random.random(len(timestamps_a))
    series_a = pd.Series(data=values_a, index=timestamps_a, name="values")

    start_b = "2023-01-02 00:00"
    end_b = "2023-01-02 03:00"
    freq_b = pd.to_timedelta(0.1, "s")
    timestamps_b = pd.date_range(
        start_b, end_b, freq=freq_b, tz="utc", inclusive="left"
    )
    values_b = np.random.random(len(timestamps_b))
    series_b = pd.Series(data=values_b, index=timestamps_b, name="values")

    series_a_and_b = pd.concat([series_a, series_b])

    # Create and upload timeseries to DataReservoi.io
    response_create = client.create(series=series_a, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]

    # Get data from DataReservoir.io (before data is appended)
    series_full_before_append = client.get(series_id, start=None, end=None)

    # Check downloaded data
    pd.testing.assert_series_equal(series_a, series_full_before_append, check_freq=False)

    # Append more data to the timeseries
    _ = client.append(series_b, series_id, wait_on_verification=True)

    # Get data from DataReservoir.io (after data is appended)
    series_full_after_append = client.get(series_id, start=None, end=None)

    # Check downloaded data
    pd.testing.assert_series_equal(series_a_and_b, series_full_after_append, check_freq=False)

    # Delete timeseries from DataReservoi.io
    client.delete(series_id)
