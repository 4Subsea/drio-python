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


.. _DataReservoir.io: https://www.datareservoir.io/
