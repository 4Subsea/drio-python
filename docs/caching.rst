Caching
###########
The ``datareservoirio.Client`` class employs a disk cache to speed up repeating timeseries downloads.
Several parameters control the behaviour of this cache, as described here:

#. ``enabled``: ``False`` if caching should not be used. Default is ``True``.
#. ``format``: format used to store timeseries chunks on disk, either 'csv' or 'csv.gz' (gzip compressed csv). Default is 'csv.gz'.
#. ``max_size``: size in megabytes that the cache is allowed to use. Default is 1024MB.
#. ``cache_root``: control the cache storage location. Default is %LOCALAPPDATA%\\reservoir_cache

Example::

    import datareservoirio as drio

    auth = drio.Authenticator('user@domain.com')

    # Initiate a client with 100GB uncompressed cache in the '\\ssd' file share
    client = drio.Client(auth, cache={'max_size': 100*1024, 'format':'csv', 'cache_root':'\\\\ssd'})
