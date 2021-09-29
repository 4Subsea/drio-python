Do's and don'ts
===============

Data size vs. memory available
------------------------------

When dealing with high-frequent data and/or long time spans, you should
keep the memory usage in mind. Having all the data in memory at the same time could
cause problems and make your script fail.

This :ref:`example<example_download_resample>` shows you how you can download
6 months of timeseries data, and calculate the 1-hour standard deviation.
In the :ref:`advanced-configuration` section you can see how to enable and configure
caching. Caching allows you to speed up repeating series downloads.

Use for loops to download data in chunks
________________________________________

It is recommended to download data in smaller chunks (such as one day, or one hour
chunks).

.. code-block:: python

    # Make a date iterator
    start_end = pd.date_range(start="2020-01-01 00:00", end="2020-02-01 00:00", freq="1H")
    start_end_iter = zip(start_end[:-1], start_end[1:])

    series_id = <your time series ID>


    # Get timeseries in chunks
    for start, end in start_end_iter:
        timeseries = client.get(series_id, start=start, end=end)

Resample data
_____________
It could be useful to resample the data. This is easily done taking advantage of `Pandas`_ capabilities:

.. code-block:: python

    # Resample using 1-minute mean
    timeseries_resampled_mean = timeseries.resample("1min").agg(np.mean)

    # Or, get the 1-minute standard deviation
    timeseries_resampled_std = timeseries.resample("1min").agg(np.std)


.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/
