.. py:currentmodule:: datareservoirio

Access time series
==================

Access existing data
--------------------
You can access time series data using the :py:meth:`Client.get` method, as long as you have the ``TimeSeriesId``  (and authorization).
Below is an example demonstrating how to access time series data, which returns a pandas Series.

.. code-block:: python

    # Get entire timeseries
    timeseries = client.get(series_id)

    # Get a slice of time series
    timeseries = client.get(series_id, start='2018-01-01 12:00:00',
                            end='2018-01-02 06:00:00')


When handling high-frequency data and/or extended timespans, it is crucial to consider memory usage. 
Accessing an excessive amount of data at once can cause your script to fail. The following is a recommended approach for accessing data in smaller chunks:

.. code-block:: python

    # Make a date iterator
    start_end = pd.date_range(start="2020-01-01 00:00", end="2020-02-01 00:00", freq="1H")
    start_end_iter = zip(start_end[:-1], start_end[1:])

    series_id = <your time series ID>


    # Get timeseries in chunks
    for start, end in start_end_iter:
        timeseries = client.get(series_id, start=start, end=end)


Access existing data with aggregation
-------------------------------------

You can also access any data you have ``TimeSeriesId`` (and authorization) for with applied aggregation using the following method. 
This approach is significantly more efficient than the :py:meth:`Client.get` method, particularly when handling large datasets.

.. code-block:: python

    # Get entire timeseries
    timeseries = client.get_samples_aggregate(series_id, start='2024-01-01',
                            end='2024-01-02', aggregation_period='15m',
                            aggregation_function='mean')

.. note::

    :py:meth:`Client.get_samples_aggregate` also returns :py:class:`pandas.Series`. The :py:mod:`start`, :py:mod:`end`, :py:mod:`aggregation_period` and :py:mod:`aggregation_function` parameters are required.   

.. important::

    Retrieving aggregated data is available only for the last 90 days.

.. warning::

    The time resolution of aggregated data is in ticks (1tick = 100 nanoseconds), while the time resolution of non-aggregated data is in nanoseconds. This may lead to discrepancies in data when comparing the two, and some datapoints might get lost when using aggregation to access data, in cases when there are multiple datapoints within the same 100 nanosecond range.
    

.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/