.. py:currentmodule:: datareservoirio

Create, edit and delete time series
===================================

Create time series
------------------

Below is an example of how to create a simple time series.

.. code-block:: python

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
information is returned:

.. code-block:: python

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

You can also store a sequence of data. However, you are required to define an increasing
integer index. This is useful when appending and updating the data later.

Store sequence:

.. code-block:: python

    # Create and store a simple sequence
    series = pd.Series(np.random.rand(10), index=np.arange(10))
    response = client.create(series)

Edit time series
----------------
You can append new data to an existing time series (and sequence).
However, any overlappinging indicies will result in overwrite/edit of existing
data:

.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/

.. code-block:: python

    dt_index = pd.date_range('2018-01-02 00:00:00', periods=10, freq='6H')
    series = pd.Series(np.random.rand(10), index=dt_index)

    series_id = response['TimeSeriesId']
    response = client.append(series, series_id)

.. important::

    The index of the series must be sorted. This is also efficient when accessing the data later.

Data verification process
-------------------------

Data that have been uploaded to `DataReservoir.io`_ will always go through a
validation process before it is made part of the series. 
By default, :py:meth:`Client.create` and :py:meth:`Client.append` will wait for
this validation process to complete successfully before appending the data to
the timeseres. This behavior can be changed using the wait_on_verification parameter:

.. code-block:: python

    response = client.create(series, wait_on_verification=False)

    response = client.append(series, series_id, wait_on_verification=False)

The result is that the data is queued for processing and the method returns
immediately. When the validation process eventually completes, the data will
be made available on the series.

.. important::

    Setting ``wait_on_verification=False`` is significantly faster, but is
    only recommended when the data is "validated" in advance. If the data
    should not pass the server-side validation the data will be ignored.


Delete data
-----------

It is only possible to delete an entire time series. Deleting a single datapoint 
is not supported.

.. danger::

    Note that deleting data is permanent and all references to ``TimeSerieId``
    is removed from the `DataReservoir.io`_ inventory.

.. code-block:: python

    client.delete(series_id)

