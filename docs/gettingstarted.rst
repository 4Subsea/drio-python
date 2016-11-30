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
    import logging
    import sys

    timeseriesclient.set_log_level(logging.DEBUG)
    timeseriesclient.logger.addHandler(logging.StreamHandler(sys.stdout))
    timeseriesclient.globalsettings.environment.set_qa()

    client = timeseriesclient.TimeSeriesClient()

    client.authenticate()

    df = pd.DataFrame({'a':np.arange(1e6)})

    result = client.upload_timeseries(df)

    print(result)

Logging and Debugging
#####################
Sometimes, when things go wrong you would like to gain some insight in what is
happening in the background. The timeseriesclient package uses pythons built-in
logging package, see the full documentation here: `https://docs.python.org/2/howto/logging.html <https://docs.python.org/2/howto/logging.html>`_.

To turn on logging there are a two steps you have to perform:

#. Set the debug level 
#. Add a logging handler
