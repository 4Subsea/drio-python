Manage metadata
================
Similar to series, you can add, update, and delete metadata. In addition, you
can assign a metadata entry to one or more series'.

Create metadata entries
-----------------------

.. code-block:: python

    # Create a metadata entry
    response = client.metadata_set('foo.bar', 'baz',
                                   vendor='Sensor Corp', type='Voltmeter')

Update/edit metadata entries
----------------------------

.. code-block:: python

    # Update/edit a metadata entry
    response = client.metadata_set('foo.bar', 'baz',
                                   vendor='Sensor Corp', type='Ampermeter')


Get metadata entries
--------------------

.. code-block:: python

    # Get a metedata entry based on namespace and key
    metadata = client.metadata_get(namespace='foo.bar', key='baz')

    # or directly by id
    metadata = client.metadata_get(metadata_id=metadata_id)

Delete metadata entries
-----------------------

.. code-block:: python

    # Delete metadata
    client.metadata_delete(metadata_id)


.. _set-metadata-on-series:

Set metadata on series
----------------------
You can assign metadata on series from existing metadata entries or just create
a new one:

.. code-block:: python

    # Existing metadata
    client.set_metadata(series_id, metadata_id=metadata_id)

    # Or create a new one during assignment
    client.set_metadata(series_id, namespace='foo.bar', key='baz',
                        vendor='Sensor Corp', type='Gyroscope')



.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/
