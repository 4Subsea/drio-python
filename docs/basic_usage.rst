Basic Usage
###########
``datareservoirio`` library comes with a ``Client`` class, which exposes all 
relevant functionality required to upload and retrieve data from DataReservoir.io.

The basic usage *workflow* is

#. Create an ``Authenticator`` instace with your credentials
#. Initiate a new ``Client``
#. Do what you need to do, upload/update/download data.

Example::

    import datareservoirio as drio
    import numpy as np
    import pandas as pd

    # The first step is to initiate an authenticator
    auth = drio.Authenticator('user@domain.com')
    # Provide password

    # Initiate a client
    client = drio.Client(auth)

    # A simple example dataset
    timevector = np.array(np.arange(0, 10e9, 1e9), dtype='datetime64[ns]')
    values = np.random.rand(10)
    data = pd.Series(values, index=timevector)

    # Upload the dataset
    result = client.create(data)

    print(result)

The response you get is a python dictionary, it should look like this::

    {
      "FileId": u'bc674968-88ke-41c4-ac35-3bdddd54e271',
      "TimeOfFirstSample": 0,
      "TimeOfLastSample": 9000000000L,
      "TimeSeriesId": u'ceb576c4-62d7-43e1-a97b-bfb9f66ddbfd'
    }

.. warning::

    Make sure to store the ``TimeSeriesId``. This ID is required to retrieve 
    the timeseries from the reservoir. If this ID is not stored, the timeseries
    will essentially be lost.

.. warning::

    DataReservoir.io require timestamps to be in UTC. To avoid conversion issues,
    ensure that your data already is in UTC before creating or appending data
    to timeseries.

