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

If you desire to have multiple separate session, it is advisable to set
a session key during authentication. This will keep the sessions (token cache)
separate:

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
configured during instantiation. The configuration is passed on as a
dictionary:

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
    client = drio.Client(
        auth,
        cache=True,
        cache_opt={"max_size": 32*1024, "cache_root": r"c:\project\drio_cache"}
    )

The cache has near disk-bound performance and will benefit greatly from fast
low-latency solid state drives.

.. warning::

    The cache is "cleaned up" during instantiation of :py:class:`Client`. If
    it is instantiated with defaults cache options, it will potentially delete
    the larger cache set up by another instance! Caution is advised!

.. note::

    If you are working with several "larger" projects at once, it may be a good
    idea to configure dedicated cache locations for each project.


Logging
-------

To simplify debugging, enable logging for the logger named 'datareservoirio'. This is especially helpful if you experience undesired behavior in your application. 

If your logging requirements are solely related to :py:mod:`datareservoirio`, you can use the following code. This will provide you with an understanding of the progress made in some 
of the processes in the package. 
In particular, when using :py:meth:`Client.get_samples_aggregate`, lowering the log level below WARNING triggers a progress bar during data collection. 
The default log level for the logger named 'datareservoirio' is WARNING.
It is recommended to use this logging.

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

* datareservoirio: top level module including configuration, authentication and client.
* datareservoirio.storage: storage module, including cache and data download.

If you need a more comprehensive logging solution that captures every interaction with the :py:mod:`requests`, :py:mod:`oauthlib`, and :py:mod:`requests-oauthlib` modules, as well as logging related to :py:mod:`datareservoirio`, you can use the code below. 
If you require logging for only one of the specific packages, you may use the pre-existing loggers integrated within :py:mod:`requests`, :py:mod:`oauthlib`, and :py:mod:`requests-oauthlib`.

.. code-block:: python

    import logging
    
    # Basic configuration of the root logger, including 'datareservoirio', 'requests', 'oauthlib' and 'requests-oauthlib'
    logging.basicConfig(format='%(asctime)s %(name)-20s %(levelname)-5s %(message)s', level=logging.DEBUG)

Instrumentation
---------------

For monitoring purposes, the external logger can be enabled to report errors and performance metrics to 4insight Team.  

To enable logging, environmental variable ``DRIO_PYTHON_APPINSIGHTS`` needs to be set to ``true``.

Using the :py:mod:`max_page_size` parameter in :py:mod:`get_samples_aggregate` method
-------------------------------------------------------------------------------------

The :py:meth:`Client.get_samples_aggregate` method uses an endpoint that has support for paging of responses. This means that instead of making one big request, it might make a series of smaller requests traversing links to next pages returned in each partial response.

Normally this is something you don't have to think about. In case you do want to change the maximum number of results returned in one page, you can use the parameter called ``max_page_size`` to alter this number. 