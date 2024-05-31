User Guide
==========
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

Data structures
---------------

`DataReservoir.io`_ works with two fundemental data structures; *series* and
*metadata*. These data structures and their Python representations are
explained below.

Series
------
A series is a one-dimensional sequence with numeric values (64-bit float) and
unique indicies (64-bit integer) (Consequently, each numeric value is natively
represented with 128-bits.) Each series is assigned a unique identifier
`TimeSeriesId` (guid) for convenient access. Furthermore, a series can be
enriched with :ref:`metadata <metadata>`.

For time series the index is interpreted as
nanoseconds since epoch (1970-01-01 00:00:00+00:00), i.e. support for
nanosencond resolution.

:py:class:`pandas.Series` objects maps perfectly to this paradigm.
:py:mod:`datareservoirio` is designed around :py:class:`pandas.Series` as it
accepts and returns series data in this format.

Higher dimensional data, such as tables (dataframes), can be broken down to
one-dimensional series', see :ref:`cookbook` for examples.

.. _metadata:

Metadata
--------
Metadata is a set of key-value pairs that can be associated with one or more
series. The purpose of metadata is to enrich series data with essential
information such as *units, origin, description, etc*. The same metadata can
also be used to search and find series data later.

`DataReservoir.io`_ employs a "schemaless" metadata store. That is, there are
no minimum requirements and you can basically add anything your heart desires.
However...

    "With great power comes great responsibility!"

    -- Uncle Ben

But, that responsibility is left to the user/app/service that uses
`DataReservoir.io`_. Simply because one-size does not fit all!

Metadata entries are organized using ``namespace`` and ``key``. A ``namespace``
can be thought of as a table and ``key`` is the row index. Then a row can have
any number of arbitrary number of columns (Note that rows in a table do not 
have to share the columns). This resembles "table storage" paradigm for those
who are familiar with that.

Thus, a ``namespace`` and ``key`` combination uniquely defines a metadata
entry in `DataReservoir.io`_. (That is, you can only have one entry in the
entire system with a given ``namespace`` and ``key`` combination). In addition,
each entry is also assigned an alias ``MetadataId`` (guid) that can be used for
direct and convenient access.

A table-like representation may look like this:

+------------------------------------------------------------+
| Namespace: vessel.electrical                               |
+------------------------+------------+-----------+----------+
| Keys                   | Units      | Vendor    | Type     |
+========================+============+===========+==========+
| Voltmeter A            | V          | Company S |          |
+------------------------+------------+-----------+----------+
| Thermometer Z          | C          |           | Analog   |
+------------------------+------------+-----------+----------+


Best practices
______________
`DataReservoir.io`_ won't enforce a particular schema when it comes to
metadata, but we can suggest smart ways of approaching it.

One simple yet effective way of creating a hierarcy and taxonomy is to use
"period" seperated names as ``namespace``. E.g.:

    * ``vessels.galactica.electrical``
    * ``service.context.sensors``
    * ``application.streams``

We found that this approach is rather easy to visualize and maps well to the
physical world.

What is it **NOT** for
______________________
Despite its flexibility, `DataReservoir.io`_ has its limitations when it comes
to metadata; it is **NOT** a general purpose database that you can dump
anything in to and it is not designed to keep track of complex hierarchical
information. The query capabilities are also kept simple and efficient by
design.

For very advanced use cases, it may be advisable to employ a purpose built
database solution (that compliments `DataReservoir.io`_ for your application).


.. _DataReservoir.io: https://www.datareservoir.io/


.. toctree::
   :maxdepth: 2
   :hidden:

   manage_series
   manage_metadata
   browse_search
   dos_donts
   advanced_config
   cookbook
