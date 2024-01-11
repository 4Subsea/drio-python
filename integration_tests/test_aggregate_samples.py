import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import datareservoirio as drio


def wait_for_data_to_appear(func, delay=3):
    count = 0
    while count == 0:
        series = func()
        count = len(series)
        if count == 0:
            time.sleep(delay)
        else:
            return series


def test_non_paged(cleanup_series):
    """
    Integration test for the samples/aggregate endpoint.

    Tests the following:
        * Creates a non-empty timeseries in DataReservoir.io.
        * Fetches it using the samples/aggregate endpoint.
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

    # Get data from DataReservoir.io using the samples/aggregate endpoint
    get_timeseries = lambda: client.get_samples_aggregate(
        series_id,
        start=start,
        end=end,
        aggregation_function="Avg",
        aggregation_period="1s",
    )

    # Wait for data to be available
    series_fetched = wait_for_data_to_appear(get_timeseries)

    # Check downloaded data
    pd.testing.assert_series_equal(series, series_fetched, check_freq=False)


def test_paged(cleanup_series):
    """
    Integration test for the samples/aggregate endpoint.

    Tests the following:
        * Creates a non-empty timeseries in DataReservoir.io.
        * Fetches it using the samples/aggregate endpoint and checks that the data is matches with what was created.
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

    # Get data from DataReservoir.io using the samples/aggregate endpoint
    check_func = lambda: client.get_samples_aggregate(
        series_id,
        start=start,
        end=end,
        aggregation_function="Avg",
        aggregation_period="1s",
        max_page_size=1000,
    )

    # Wait for data to be available
    series_fetched = wait_for_data_to_appear(check_func)

    # Check downloaded data
    pd.testing.assert_series_equal(series, series_fetched, check_freq=False)
