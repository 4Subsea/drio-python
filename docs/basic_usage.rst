Basic Usage
###########

The :py:mod:`datareservoirio` library provide access to `DataReservoir.io`_,
and is a native Python experience. The following features are supported through the
Python API:

* Manage series (time series or sequences):

    * Access existing series
    * Create and upload new series
    * Edit and append to existing series
    * Delete series

* Manage metadata

    * Set metadata
    * Edit existing metadata
    * Delete metadata

* Browse and search for series based on metadata

.. py:currentmodule:: datareservoirio

All of the above functionality are exposed as high-level methods in the 
:py:class:`Client` class. The general *workflow* can be summarized as:

#. Authenticate
#. Instantiate a new ``client`` (using :py:class:`Client`)
#. Go... Data is waiting for you!

Examples
********

Manage series
=============

Store series data
-----------------
::

    # Create and store a simple time series 

    import datareservoirio as drio
    import numpy as np
    import pandas as pd

    auth = drio.Authenticator()
    # Follow instructions to authenticate

    client = drio.Client(auth)

    dt_index = pd.date_range('2018-01-01 00:00:00', periods=10, freq='6H')
    series = pd.Series(np.random.rand(10), index=dt_index)

    response = client.create(series)

If the request was successful, a Python dictionary containing essential
information is returned::

    {
        'FileId': '2465e7c8-7a5e-4602-bb3b-a5a01382aa1f',
        'TimeSeriesId': '8050a49c-8b61-448d-bdbb-51248a23dbd9',
        'TimeOfFirstSample': 1514764800000000000,
        'TimeOfLastSample': 1514959200000000000
    }

.. important::

    ``TimeSeriesId`` is the unique identifier (guid) assigned to the series.
    It is recommended that you :ref:`add some metadata to the series <set-metadata-on-series>` so that it is
    easier to find at a later time, or at least store the ``TimeSeriesId`` for later reference.

.. important::

    `DataReservoir.io`_ works with UTC-time. All datetime-like objects are
    converted to UTC and therefore, time zone information is lost when data is
    stored in `DataReservoir.io`_.

You can also store a sequence of data. However, you are required to define an
integer index. (This is useful when appending and updating the data later.)

Store sequence::

    # Create and store a simple sequence
    series = pd.Series(np.random.rand(10), index=np.arange(10))
    response = client.create(series)


Edit and append data
--------------------

You can always append new data to an existing time series (and sequence).
However, any overlappinging indecies will result in overwrite/edit of existing
data::


    dt_index = pd.date_range('2018-01-02 00:00:00', periods=10, freq='6H')
    series = pd.Series(np.random.rand(10), index=dt_index)

    series_id = response['TimeSeriesId']
    response = client.append(series, series_id)


Data verification process
-------------------------

Data that have been uploaded to `DataReservoir.io`_ will always go through a
validation process before it is made part of the series. 
By default, :py:func:`Client.create` and :py:func:`Client.append` will wait for
this validation process to complete successfully before appending the data to
the timeseres. This behavior can be changed using the wait_on_verification parameter:

    response = client.create(series, wait_on_verification=False)

    response = client.append(series, series_id, wait_on_verification=False)

The result is that the data is queued for processing and the method returns
immediately. When the validation process eventually completes, the data will
be made available on the series.

.. important::

    Setting `wait_on_verification` to `False` is significantly faster, but is
    only recommended when the data is "validated" in advance. If the data
    should not pass the server-side validation the data will be ignored.


Access existing data
--------------------

You can access any data you have ``TimeSeriesId`` (and authorization) for::

    # Get entire timeseries
    timeseries = client.get(series_id)

    # Get a slice of time series
    timeseries = client.get(series_id, start='2018-01-01 12:00:00',
                            end='2018-01-02 06:00:00')

    # Get a sequence
    sequence = client.get(series_id, convert_date=False)

.. note::

    :py:func:`Client.get` returns :py:class:`pandas.Series`.


Delete data
-----------

Note that deleting data is permanent and all references to ``TimeSerieId``
is removed from the `DataReservoir.io`_ inventory::

    client.delete(series_id)


Manage metadata
================
Similar to series, you can add, update, and delete metadata. In addition, you
can assign a metadata entry to one or more series'.

Create metadata entries
-----------------------
::

    # Create a metadata entry
    response = client.metadata_set('foo.bar', 'baz',
                                   vendor='Sensor Corp', type='Voltmeter')

Update/edit metadata entries
----------------------------
::

    # Update/edit a metadata entry
    response = client.metadata_set('foo.bar', 'baz',
                                   vendor='Sensor Corp', type='Ampermeter')


Get metadata entries
--------------------
::

    # Get a metedata entry based on namespace and key
    metadata = client.metadata_get(namespace='foo.bar', key='baz')

    # or directly by id
    metadata = client.metadata_get(metadata_id=metadata_id)

Delete metadata entries
-----------------------
::

    # Delete metadata
    client.metadata_delete(metadata_id)


.. _set-metadata-on-series:

Set metadata on series
----------------------
You can assign metadata on series from existing metadata entries or just create
a new one::

    # Existing meteadata
    client.set_metadata(series_id, metadata_id=metadata_id)

    # Or create a new one during assignment
    client.set_metadata(series_id, namespace='foo.bar', key='baz',
                        vendor='Sensor Corp', type='Gyroscope')


Browse and search
=================

Browse metadata
---------------
You can browse metadata, and search for metadata and series data through
:py:mod:`datareservoirio`. Lets see how you can browse metadata entries::

    # List all available namespaces
    namespaces = client.metadata_browse(namespace=None, key=None)

    # List all keys under a give namespace
    keys = client.metadata_browse(namespace='foo.bar')

    # List all namespaces that contains a given namespace
    key_namspaces = client.metadata_browse(key='baz')

    # Get a specific entry (dict)
    keys = client.metadata_browse(namespace='foo.bar', key='baz')

Search for metadata
-------------------
You can also search for metadata::

    # Search for *namespace* OR *key*
    metadata_list = client.metadata_search('foo.bar', 'baz', conjunctive=False)

    # Search for *namespace* AND *key*
    metadata_list = client.metadata_search('foo.bar', 'baz', conjunctive=True)

.. note::

    The search is "fuzzy" as it looks for matches with
    "wildcard + search term + wildcard". It is recommended to be as specific as
    possible for best performance.

Search for series
-----------------
In addition, you can search directly for series based on metadata associated
with it::

    # Get all series that have metadata that satisfies a search:
    # namespace + key* + name + value (optional)

    series_ids_list = client.search('foo.bar', 'baz', 'sensor_vendor')

    series_ids_dict = client.search('foo.bar', 'baz', 'sensor_vendor',
                                    value='Sensor Corp')


Do's and don'ts
***************

Data size vs. memory available
==============================

When dealing with high-frequent data and/or long time spans, you should
keep the memory usage in mind. Having all the data in memory at the same time could
cause problems and make your script fail.

This :ref:`example<example_download_resample>` shows you how you can download
6 months of timeseries data, and calculate the 1-hour standard deviation.
In the :ref:`Advanced Configuration` section you can see how to enable and configure
caching. Caching allows you to speed up repeating series downloads.

Use for loops to download data in chunks
----------------------------------------
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

Resample using pandas
---------------------
It could be useful to resample the data. This is easily done with pandas:

.. code-block:: python

    # Resample using 1-minute mean
    timeseries_resampled_mean = timeseries.resample("1min").agg(np.mean)

    # Or, get the 1-minute standard deviation
    timeseries_resampled_std = timeseries.resample("1min").agg(np.std)


.. _DataReservoir.io: https://www.datareservoir.io/
