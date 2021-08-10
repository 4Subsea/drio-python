.. _cookbook:

Cookbook
########
There are many complex yet common use cases for :py:mod:`datareservoirio`. We
have collected some of them in this section. If you have suggestions on what
more we can add to this section, please :ref:`let us know! <support>`


Visualize data
**************
It is really easy to visualize data with `Matplotlib`_::

    import datareservoirio as drio
    import matplotlib.pyplot as plt


    auth = drio.Authenticator()
    client = drio.Client(auth)

    data = client.get(series_id, start='2018-02-14', end='2018-02-17')

    plt.figure()
    plt.plot(data)


Save data to file
*****************
Sometimes you may want to dump data to file (Don't worry, we won't judge you)::

    import datareservoirio as drio


    auth = drio.Authenticator()
    client = drio.Client(auth)

    data = client.get(series_id, start='2018-02-14', end='2018-02-17')
    data.to_csv('path')


.. note::
    Data is dumped to file using the built-in `Pandas`_ functionality. Thus,
    you can choose many different file-formats where CSV is just one of them.


Work with higher dimensional data
*********************************
Let's see how you can upload and store a higher dimensional dataset::

    import datareservoirio as drio


    auth = drio.Authenticator()
    client = drio.Client(auth)

    data_dict = {
        'x': np.random.rand(10),
        'y': np.random.rand(10),
        'z': np.random.rand(10),
    }

    df = pd.DataFrame(data_dict, index = np.arange(10))

    series_ids = {}
    for name, col in df.iteritems():
        response = client.create(series=col)
        series_ids[name] = response['TimeSeriesId']


Now it will be possible to reconstruct the original dataframe since we have all
the ``TimeSeriesId`` s::

    data_dict = {
        name: client.get(series_id, convert_date=False) 
        for name, series_id in series_ids.items()
        }

    df = pd.DataFrame(data_dict)


.. _example_download_resample:

Work with large amount of data
******************************
When working with large data sizes (long time spans and/or high sampling frequency),
it is often useful to download data in chunks and resample so that you don't have
all the data in memory at the same time. Let's see how you can download 6 months of
data and get the 1-hour standard deviation::

    import numpy as np
    import datareservoirio as drio


    auth = drio.Authenticator()
    client = drio.Client(auth)

    start_end = pd.date_range(start="2020-01-01 00:00", end="2020-06-01 00:00", freq="1D")
    start_end_iter = zip(start_end[:-1], start_end[1:])

    result = pd.Series()
    for start, end in start_end_iter:
        timeseries = client.get(series_id, start=start, end=end)

        result = pd.concat([result, timeseries.resample("1H").agg(np.std)])









.. _Matplotlib: https://matplotlib.org/
.. _Pandas: https://pandas.pydata.org/
