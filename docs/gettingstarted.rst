Getting Started
###############

Installation
************
The python wheel, the installable file, is located at ``\\FIL-ASK-004\python``.
The easiest way to install the package is via pip, like all our other packages::

   pip install timeseriesclient -f \\fil-ask-004\python

Basic Usage
###########
As *user* the main object you will be working with from the library is the 
class ``TimeSeriesClient``. This class exposes all relevant functionality
to upload and retrieve data from the data reservoir.

The basic usage *workflow* is

#. Create a new ``TimeSeriesClient``
#. Authenticate yourself with your 4Subsea credentials
#. Do what you need to do, upload/update/download time series.

basic usage::

    import timeseriesclient
    import numpy as np
    import pandas as pd

    # Create a new client to work with
    client = timeseriesclient.TimeSeriesClient()

    # First step is to authenticate.
    # The client will ask for your username and password
    client.authenticate()

    # A simple example dataset
    timevector = np.array(np.arange(0, 10e9, 1e9), dtype='datetime64[ns]')
    values = np.random.rand(10)
    df = pd.DataFrame({'values' : values}, index=timevector)

    # Upload the dataset
    result = client.create(df)

    print(result)

The response you get is a python dictionary, it should look like this::

    {
      "FileId": 0,
      "TimeOfFirstSample": 0,
      "TimeOfLastSample": 0,
      "TimeSeriesId": 0,
      "FileStatus": 0,
      "ReferenceTime": "2016-12-01T11:18:57.448Z",
      "LastModifiedByEmail": "string",
      "Created": "2016-12-01T11:18:57.448Z",
      "LastModified": "2016-12-01T11:18:57.448Z",
      "CreatedByEmail": "string"
    } 

.. warning::

    Make sure to check the response for errors. Are the times for the first 
    and last sample reasonable/correct?

    And most of all. Make sure to store the ``TimeSeriesId``. This is ID is 
    required to retrieve the timeseries from the reservoir. If this ID is not 
    stored, the timeseries will essentially be lost.

.. note::

    As soon as the backend for storing metadata is ready, the responsibility 
    for taking care of the ``TimeSeriesId`` will be taken away from the user.


Logging and Debugging
#####################
Sometimes, when things go wrong you would like to gain some insight in what is
happening in the background. The timeseriesclient package uses pythons built-in
logging package, see the full documentation here: `https://docs.python.org/2/howto/logging.html <https://docs.python.org/2/howto/logging.html>`_.

To turn on logging there are a two steps you have to perform:

#. Set the debug level 
#. Add a logging handler
