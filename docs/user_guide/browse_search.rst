Browse and search
=================

Browse metadata
---------------
You can browse metadata, and search for metadata and series data through
:py:mod:`datareservoirio`. Lets see how you can browse metadata entries:

.. code-block:: python

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
You can also search for metadata given a namespace and key:

.. code-block:: python

    # Search for metadata given *namespace* and *key*
    metadata_list = client.metadata_search('foo.bar', 'baz')


.. note::

    The namespace argument must be a perfect match. They key argument can be "begins with", meaning that 
    the search looks for matches with "key + wildcard". It is recommended to be as specific as
    possible for best performance.


Search for series
-----------------
In addition, you can search directly for series based on metadata associated
with it:

.. code-block:: python

    # Get all series that have metadata that satisfies a search:
    # namespace + key* + name + value (optional)

    series_ids_list = client.search('foo.bar', 'baz', 'sensor_vendor')

    series_ids_dict = client.search('foo.bar', 'baz', 'sensor_vendor',
                                    value='Sensor Corp')


.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/
