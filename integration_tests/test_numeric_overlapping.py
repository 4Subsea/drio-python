import os

import numpy as np
import pandas as pd
import pytest
from requests import HTTPError

import datareservoirio as drio


def test_numeric_overlapping(cleanup_series):
    """
    Integration test for overlapping numeric data.

    Tests the following:
        * Create a timeseries A in DataReservoir.io.
        * Append overlapping timeseries B.
        * Append overlapping timeseries C.

    """

    # Initialize client
    auth_session = drio.authenticate.ClientAuthenticator(
        os.getenv("DRIO_CLIENT_ID"), os.getenv("DRIO_CLIENT_SECRET")
    )
    client = drio.Client(auth_session)

    # Create some dummy data (overlapping)
    start_full = "2022-12-30 00:00"
    end_full = "2023-01-02 00:00"
    freq = pd.to_timedelta(10, "s")
    index_full = pd.date_range(
        start_full, end_full, freq=freq, tz="utc", inclusive="left"
    )

    n = len(index_full)
    index_a = index_full[: n // 2]
    index_b = index_full[n // 4 : n // 2]
    index_c = index_full[n // 3 :]

    values_a = np.random.random(len(index_a))
    values_b = np.random.random(len(index_b))
    values_c = np.random.random(len(index_c))

    series_a = pd.Series(data=values_a, index=index_a, name="values")
    series_b = pd.Series(data=values_b, index=index_b, name="values")
    series_c = pd.Series(data=values_c, index=index_c, name="values")

    df_a = series_a.to_frame()
    df_b = series_b.to_frame()
    df_c = series_c.to_frame()

    df_ab = df_b.combine_first(df_a)  # B overlapping A
    df_abc = df_c.combine_first(df_ab)  # C overlapping B overlapping A

    # Create and upload timeseries A to DataReservoir.io
    response_create = client.create(series=series_a, wait_on_verification=True)
    series_id = response_create["TimeSeriesId"]
    cleanup_series.add(series_id)

    # Append timeseries B
    _ = client.append(series_b, series_id, wait_on_verification=True)

    # Get data from DataReservoir.io
    series_ab_out = client.get(series_id, start=None, end=None)

    # Append timeseris C
    _ = client.append(series_c, series_id, wait_on_verification=True)

    # Get data from DataReservoir.io
    series_abc_out = client.get(series_id, start=None, end=None)

    # Check downloaded data
    series_ab_expect = df_ab["values"]
    series_abc_expect = df_abc["values"]
    pd.testing.assert_series_equal(series_ab_out, series_ab_expect, check_freq=False)
    pd.testing.assert_series_equal(series_abc_out, series_abc_expect, check_freq=False)

    # Delete timeseries from DataReservoir.io
    client.delete(series_id)

    # Check that the timeseries is deleted
    with pytest.raises(HTTPError):
        _ = client.get(series_id)
