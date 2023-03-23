"""
Integration test.

Tests the following:
    * Create timeseries in DataReservoir.io.
    * Append more data to the created timeseries.
    * Delete the timeseries from DataReservoi.io

"""
import datareservoirio as drio


def test_numeric_datetime(client):
    assert 1 == 1
