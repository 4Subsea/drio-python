Browse and search
=================

Browse metadata
---------------
You can browse metadata, and search for metadata and series data through
:py:mod:`datareservoirio`. Let's see how you can browse metadata entries:

.. code-block:: python

    # List all available namespaces
    namespaces = client.metadata_browse(namespace=None)

    # List all keys under a give namespace
    keys = client.metadata_browse(namespace='foo.bar')


Search for metadata
-------------------
You can also search for metadata given a namespace and key:

.. code-block:: python

    # Search for metadata given namespace and key*
    metadata_list = client.metadata_search('foo.bar', 'baz')


.. note::

    The namespace argument must be an exact match. They key argument can be "begins with", meaning that 
    the search looks for matches with "key + wildcard". It is recommended to be as specific as
    possible for best performance.


Search for series
-----------------
In addition, you can search directly for time series based on metadata associated
with it:

.. code-block:: python

    # Get all series that have metadata that satisfies a search:
    # namespace + key* (optional) + name (optional) + *value* (optional)

    series_ids_list = client.search('foo.bar', 'baz', 'sensor_vendor')

    series_ids_dict = client.search('foo.bar', 'baz', 'sensor_vendor',
                                    'Sensor Corp')


The search function has one required argument (namespace), the rest are optional:

.. list-table::
   :widths: 25 75

   * - namespace
     - required, must be exact match   
   * - key
     - optional, defaults to None. Can be "begins with"
   * - name
     - optional, defaults to None. Must be exact match
   * - value
     - optional, defaults to None. Can be "begins with" or "ends with" or a combination. 



.. note::
    
    The arguments in client.search are hierarchical and starts from the left. If one argument is set to None, 
    all proceeding arguments will be treated as none. For example, client.search('foo.bar', None, 'sensor_vendor')
    will return the same result as client.search('foo.bar')


Series information
------------------
The client.info() method returns a dict containing all available metadata for a given time series:

.. code-block:: python

    ts_id = '0f10c985-ad2c-409f-aa5d-e592b11b8526'
    info = client.info(ts_id)



.. _DataReservoir.io: https://www.datareservoir.io/
.. _Pandas: https://pandas.pydata.org/
