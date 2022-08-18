.. py:currentmodule:: datareservoirio
.. _advanced-configuration:

Advanced configuration
======================

Authentication
--------------

Single user / interactive
_________________________

The default and recommended method for authentication for users is using
:py:class:`Authenticator`. You will be guided to your organizations login
webpage, and login as usual. (We will not see or store your credentials!). Once
authenticated, you can choose to re-use your (valid) access token (i.e. not be
prompted to authenticate next time) or re-authenticate everytime:

.. code-block:: python

    import datareservoirio as drio

    # Re-use (valid) access token from last sesssion 
    auth = drio.Authenticator()

    # or re-authenticate
    auth = drio.Authenticator(auth_force=True)

.. caution::

    Users on shared computers should always re-authenticate since access token
    from a different user may unintentionally be used.

If you desire to have multiple seperate session, it is advisable to set
a session key during authetication. This will keep the sessions (token cache)
seperate:

.. code-block:: python

    auth_0 = drio.Authenticator(session_key="my_unique_session_0")
    auth_1 = drio.Authenticator(session_key="my_unique_session_1")

Service account / non-interactive client
________________________________________

If you require client/backend type of authentication flow where user interaction
is not feasible nor desired, you can use the
:py:class:`authenticate.ClientAuthenticator`:

.. code-block:: python

    import datareservoirio as drio

    auth = drio.authenticate.ClientAuthenticator("my_client_id", "my_client_secret")

:ref:`Contact us <support>` and we will provide you the specifics.


Caching
-------
The :py:class:`Client` class employs a disk cache to speed up repeating series
downloads. Beside turning the cache on and off, several aspects of it can be
configured during instantiation. The configuration are passed on as a
dictionary:

* ``format``: format used to store series on disk, either 'parquet' or 'csv'. Default is 'parquet'.
* ``max_size``: size in megabytes that the cache is allowed to use. Default is 1024MB.
* ``cache_root``: control the cache storage location. Default locations are:
    
    * Windows: ``%LOCALAPPDATA%\\datareservoirio\\Cache``
    * Linux: ``~/.cache/datareservoirio`` (XDG default)
    * MacOs: ``~/Library/Caches/datareservoirio``

Example:

.. code-block:: python

    import datareservoirio as drio


    auth = drio.Authenticator()

    # Initiate a client with 32GB cache in the 'c:\project\drio_cache'
    client = drio.Client(auth, cache=True,
                         cache_opt={'format': 'parquet', 'max_size': 32*1024,
                                    'cache_root': r'c:\project\drio_cache'})

The cache has near disk-bound performance and will benefit greatly from fast
low-latency solid state drives.

.. warning::

    The cache is "cleaned up" during instantiation of :py:class:`Client`. If
    it is instantiated with defaults cache options, it will potentially delete
    the larger cache set up by another instance! Caution is adviced!

.. note::

    If you are working with several "larger" projects at once, it may be a good
    idea to configure dedicated cache locations for each project.


Logging
-------

To simplify debugging, enable logging for the logger named 'datareservoirio'.

.. code-block:: python

    import logging
    
    # Basic configuration of the root logger, including 'datareservoirio'
    logging.basicConfig(format='%(asctime)s %(name)-20s %(levelname)-5s %(message)s', level=logging.INFO)

.. code-block:: python

    import logging
    import datareservoirio
    
    # Configure desired log level specifically for 'datareservoirio'
    logger = logging.getLogger('datareservoirio')
    logger.setLevel(logging.DEBUG)
    
    # Short-hand for the above
    datareservoirio.set_log_level(logging.DEBUG)

.. code-block:: python

    import logging
    
    # Advanced configuration allowing control of log level, message format and output handler
    logger = logging.getLogger('datareservoirio')
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-20s %(levelname)-5s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

The following log names can be used to fine-tune the desired log output:

* datareservoirio: top level module including configuration, authentication and client
* datareservoirio.storage: storage module, including cache and data download
* datareservoirio.rest_api: API module with logging of request parameters and responses

If you require even more detailed logging, consider using loggers from
:py:mod:`requests`, :py:mod:`oauthlib`, :py:mod:`requests-oauthlib` and :py:mod:`azure-storage-blob`
