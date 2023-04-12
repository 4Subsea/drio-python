import os
import time

import numpy as np
import pandas as pd
import pytest
from requests import HTTPError

import datareservoirio as drio


def test_numeric_cached(cleanup_series, tmp_path):
    """
    Integration test for creating/appending/deleting timeseries with numeric values
    and datetime index (NB! caching enabled).

    Tests the following:
        * Create a timeseries in DataReservoir.io.
        * Append more data to the created timeseries.
        * Delete the timeseries from DataReservoir.io.
        * Check that data is cached.

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

    # Check that the cache folder is made, and that it is empty
    assert CACHE_PATH.exists()
    assert len(list(CACHE_PATH.iterdir())) == 0

    # Create some dummy data
    start_a = "2022-12-30 00:00"
    end_a = "2023-01-02 00:00"
    freq_a = pd.to_timedelta(10, "s")
    index_a = pd.date_range(start_a, end_a, freq=freq_a, tz="utc", inclusive="left")
    series_a = pd.Series(
        data=np.random.random(len(index_a)), index=index_a, name="values"
    )

    start_b = "2023-01-02 00:00"
    end_b = "2023-01-02 03:00"
    freq_b = pd.to_timedelta(0.1, "s")
    index_b = pd.date_range(start_b, end_b, freq=freq_b, tz="utc", inclusive="left")
    series_b = pd.Series(
        data=np.random.random(len(index_b)), index=index_b, name="values"
    )

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

    # Check that the cache folder now contains three days of data
    assert len(list(CACHE_PATH.iterdir())) == 3

    # Append more data to the timeseries
    _ = client.append(series_b, series_id, wait_on_verification=True)

    # Get data from DataReservoir.io (after data is appended)
    series_full_after_append = client.get(series_id, start=None, end=None)

    # Check downloaded data
    pd.testing.assert_series_equal(
        series_a_and_b, series_full_after_append, check_freq=False
    )

    # Check that the cache folder now contains four days of data
    assert len(list(CACHE_PATH.iterdir())) == 4

    # Get data between two dates from DataReservoir.io
    start = pd.to_datetime("2023-01-01 00:00", utc=True)
    end = pd.to_datetime("2023-01-01 03:00", utc=True)
    delta = pd.to_timedelta(1, "ns")
    series_partial = client.get(series_id, start=start, end=end)

    # Check downloaded data
    pd.testing.assert_series_equal(
        series_a_and_b.loc[start : end - delta], series_partial, check_freq=False
    )

    # Check that data is read from cache
    time_before_get = time.time()
    _ = client.get(series_id, start=None, end=None)
    time_after_get = time.time()
    for cache_file_i in CACHE_PATH.iterdir():
        time_access_file_i = os.path.getatime(cache_file_i)   # last access time
        assert time_before_get < time_access_file_i < time_after_get

    # Delete timeseries from DataReservoir.io
    client.delete(series_id)

    # Check that the timeseries is deleted
    with pytest.raises(HTTPError):
        _ = client.get(series_id)
