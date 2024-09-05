.. py:currentmodule:: datareservoirio

Access time series
==================

Access existing data
-------------------------------------

You can access any data for which you have the ``TimeSeriesId`` (and authorization). It is possible to 
query aggregated data directly, e.g. you can query 1 minute average values for a specified period. The finest available aggregation period
is *"tick"* (100 nanoseconds). 


.. code-block:: python
    
    # set up
    import datareservoirio as drio
    import numpy as np
    import pandas as pd

    auth = drio.Authenticator()
    # Follow instructions to authenticate

    client = drio.Client(auth)

    # Get timeseries data resampled to 15 minutes average for selected period
    timeseries = client.get_samples_aggregate(series_id, 
                            start='2024-01-01', end='2024-01-02', 
                            aggregation_period='15m',
                            aggregation_function='mean')

    # Get all data for selected time period
    timeseries = client.get_samples_aggregate(series_id, 
                            start='2024-01-01', end='2024-01-02', 
                            aggregation_period='tick',
                            aggregation_function='mean')

.. note::

    :py:meth:`Client.get_samples_aggregate` returns a :py:class:`pandas.Series`. The :py:mod:`start`, :py:mod:`end`, :py:mod:`aggregation_period` and :py:mod:`aggregation_function` parameters are required.   

.. important::

    Time series data is archived 90 days after the upload. To access archived data directly, you can use 
    the :py:meth:`Client.get` method, but the data can also be restored by contacting :ref:`support <support>`.


Access archived data
--------------------
You can access time series data using the :py:meth:`Client.get` method, as long as you have 
the ``TimeSeriesId`` (and authorization). Note that this method only returns the raw data, and 
does not support aggregation. Below is an example demonstrating how to access archived time 
series data. We strongly recommended to use the 
:py:meth:`Client.get_samples_aggregate` as long as the data was uploaded within the last 90 days,
or contact support to restore it.

.. code-block:: python

    # Get entire timeseries
    timeseries = client.get(series_id)

    # Get a slice of time series
    timeseries = client.get(series_id, start='2018-01-01 12:00:00',
                            end='2018-01-02 06:00:00')


.. warning::

    The time resolution of aggregated data is in ticks (1tick = 100 nanoseconds), while the time resolution of non-aggregated data is in nanoseconds. This may lead to discrepancies in data when comparing the two, and some datapoints might get lost when using aggregation to access data, in cases when there are multiple datapoints within the same 100 nanosecond range.


.. tip::
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


.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/