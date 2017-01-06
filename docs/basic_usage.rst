Basic Usage
###########
As *user* the main object you will be working with from the library is the 
class ``TimeSeriesClient``. This class exposes all relevant functionality
to upload and retrieve data from the data reservoir.

The basic usage *workflow* is

#. Create a new ``TimeSeriesClient``
#. Authenticate yourself with your 4Subsea credentials
#. Do what you need to do, upload/update/download time series.

Example::

    import timeseriesclient
    import numpy as np
    import pandas as pd

    # The first step is to initiate an authenticator
    auth = timeseriesclient.Authenticator('user@domain.com')
    # Provide password

    # Initiate a client
    client = timeseriesclient.TimeSeriesClient(auth)

    # A simple example dataset
    timevector = np.array(np.arange(0, 10e9, 1e9), dtype='datetime64[ns]')
    values = np.random.rand(10)
    df = pd.DataFrame({'values' : values}, index=timevector)

    # Upload the dataset
    result = client.create(df)

    print(result)

The response you get is a python dictionary, it should look like this::

    {
      "FileId": u'bc674968-88ke-41c4-ac35-3bdddd54e271',
      "TimeOfFirstSample": 0,
      "TimeOfLastSample": 9000000000L,
      "TimeSeriesId": u'ceb576c4-62d7-43e1-a97b-bfb9f66ddbfd'
    }

.. warning::

    And most of all. Make sure to store the ``TimeSeriesId``. This is ID is 
    required to retrieve the timeseries from the reservoir. If this ID is not 
    stored, the timeseries will essentially be lost.
