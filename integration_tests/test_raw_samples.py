import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import datareservoirio as drio


def wait_for_data_to_appear(func, delay=3, timeout=120):
    deadline = time.monotonic() + timeout
    while True:
        series = func()
        if len(series):
            return series
        if time.monotonic() >= deadline:
            raise TimeoutError("Timed out waiting for raw samples to appear")
        time.sleep(delay)


def test_non_paged(cleanup_series):
    """
    Integration test for the samples/raw endpoint.

    Tests the following:
        * Creates a non-empty timeseries in DataReservoir.io.
        * Fetches it using the samples/raw endpoint.
        * Checks that the data matches with what was created.
    """

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(auth_session, cache=False)

    # Create some dummy data
    # Relative from today, because we have the 3 month limitation
    end = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    freq = pd.to_timedelta(1, "s")
    index = pd.date_range(
        start, end, freq=freq, tz="utc", inclusive="left", name="index"
    )
    series = pd.Series(data=np.random.random(len(index)), index=index, name="values")
    response_create = client.create(series=series, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]

    # For cleaning up after test runs
    cleanup_series.add(series_id)

    # Get data from DataReservoir.io using the samples/raw endpoint
    get_timeseries = lambda: client.get_samples_raw(
        series_id,
        start=start,
        end=end,
    )

    # Wait for data to be available
    series_fetched = wait_for_data_to_appear(get_timeseries)

    # Check downloaded data
    pd.testing.assert_series_equal(series, series_fetched, check_freq=False)


def test_paged(cleanup_series):
    """
    Integration test for the samples/raw endpoint.

    Tests the following:
        * Creates a non-empty timeseries in DataReservoir.io.
        * Fetches it using the samples/raw endpoint and checks that the data is matches with what was created.
        * uses a maxPageSize of 1000 which means the response will be paged (into 4 pages)
    """

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(auth_session, cache=False)

    # Create some dummy data
    # Relative from today, because we have the 3 month limitation
    end = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    freq = pd.to_timedelta(1, "s")

    index = pd.date_range(
        start, end, freq=freq, tz="utc", inclusive="left", name="index"
    )
    series = pd.Series(data=np.random.random(len(index)), index=index, name="values")
    response_create = client.create(series=series, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]

    # For cleaning up after test runs
    cleanup_series.add(series_id)

    # Get data from DataReservoir.io using the samples/raw endpoint
    check_func = lambda: client.get_samples_raw(
        series_id,
        start=start,
        end=end,
        max_page_size=1000,
    )

    # Wait for data to be available
    series_fetched = wait_for_data_to_appear(check_func)

    # Check downloaded data
    pd.testing.assert_series_equal(series, series_fetched, check_freq=False)


def test_raw_equals_aggregate_tick(cleanup_series):
    """
    Integration test comparing the samples/raw and samples/aggregate endpoints.

    Tests the following:
        * Creates a non-empty timeseries of doubles in DataReservoir.io.
        * Fetches it using the samples/raw endpoint.
        * Fetches it using the samples/aggregate endpoint with "tick" period.
        * Checks that both are equal, since aggregating on tick level returns the raw data.
    """

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(auth_session, cache=False)

    # Create some dummy data
    # Relative from today, because we have the 3 month limitation
    end = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    freq = pd.to_timedelta(100, "ms")
    index = pd.date_range(
        start, end, freq=freq, tz="utc", inclusive="left", name="index"
    )
    data = np.random.random(len(index))
    # Include whole number doubles, e.g. 2.0, to check they are not returned as integers
    data[::10] = np.round(data[::10] * 100)
    series = pd.Series(data=data, index=index, name="values")
    response_create = client.create(series=series, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]

    # For cleaning up after test runs
    cleanup_series.add(series_id)

    # Get data from DataReservoir.io using both endpoints
    get_raw = lambda: client.get_samples_raw(series_id, start=start, end=end)
    get_aggregate = lambda: client.get_samples_aggregate(
        series_id,
        start=start,
        end=end,
        aggregation_function="mean",
        aggregation_period="tick",
    )

    # Wait for data to be available
    series_raw = wait_for_data_to_appear(get_raw)
    series_aggregate = wait_for_data_to_appear(get_aggregate)

    # Check downloaded data
    assert series_raw.dtype == "float64"
    pd.testing.assert_series_equal(series_raw, series_aggregate, check_freq=False)
    pd.testing.assert_series_equal(series, series_raw, check_freq=False)
