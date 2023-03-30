import os

import numpy as np
import pandas as pd
import pytest
from requests import HTTPError

import datareservoirio as drio


def test_numeric_datetime_caching(cleanup_series, tmp_path):
    """
    Integration test with caching enabled for creating/appending/deleting timeseries
    with numeric values and datetime index.

    Tests the following:
        * Create a timeseries in DataReservoir.io.
        * Download the data
        * Download the data again, to get it from cache.
        * Delete the timeseries from DataReservoir.io

    """

    STOREFORMATVERSION = "v3"
    CACHE_ROOT = tmp_path / ".cache"
    CACHE_PATH = CACHE_ROOT / STOREFORMATVERSION

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(
        auth_session, cache=True, cache_opt={"cache_root": CACHE_ROOT, "max_size": 1024}
    )

    # Check that the cache folder is made
    assert CACHE_PATH.exists()

    # Create some dummy data
    start = "2022-12-30 00:01"
    end = "2023-01-02 23:59"
    freq = pd.to_timedelta(10, "s")
    index = pd.date_range(start, end, freq=freq, tz="utc", inclusive="left")
    series = pd.Series(data=np.random.random(len(index)), index=index, name="values")

    # Create and upload timeseries to DataReservoir.io
    response_create = client.create(series=series, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]
    cleanup_series.add(series_id)

    # Check that the cache folder is empty
    assert len(list(CACHE_PATH.iterdir())) == 0

    # Get data from DataReservoir.io (before data is cached)
    series_full_before_cached = client.get(series_id, start=None, end=None)

    # Get data from DataReservoir.io (before data is cached)
    series_full_before_cached = client.get(series_id, start=None, end=None)

    # Check that the cache folder is not empty anymore
    print(len(list(CACHE_PATH.iterdir())))
    assert len(list(CACHE_PATH.iterdir())) != 0

    # Check downloaded data
    pd.testing.assert_series_equal(series, series_full_before_cached, check_freq=False)

    # Get data from DataReservoir.io (after data is cached)
    series_full_after_cached = client.get(series_id, start=None, end=None)

    # Check downloaded data
    pd.testing.assert_series_equal(series, series_full_after_cached, check_freq=False)

    # # Get data between two dates from DataReservoir.io
    # start = pd.to_datetime("2023-01-01 00:00", utc=True)
    # end = pd.to_datetime("2023-01-01 03:00", utc=True)
    # delta = pd.to_timedelta(1, "ns")
    # series_partial = client.get(series_id, start=start, end=end)

    # # Check downloaded data
    # pd.testing.assert_series_equal(
    #     series_a_and_b.loc[start : end - delta], series_partial, check_freq=False
    # )

    # Delete timeseries from DataReservoir.io
    client.delete(series_id)

    # Check that the timeseries is deleted
    with pytest.raises(HTTPError):
        _ = client.get(series_id)
