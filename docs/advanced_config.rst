Advanced Configuration
######################

.. py:currentmodule:: datareservoirio

Authentication
**************
The default and recommended method for authentication is using
:py:class:`Authenticator`. You will be guided to your organizations login
webpage, and login as usual. (We will not see or store your credentials!). Once
authenticated, you can choose re-use your (valid) access token (i.e. not be
prompted to authenticate next time) or re-authenticate everytime::

    import datareservoirio as drio

    # Re-use (valid) access token from last sesssion 
    auth = drio.Authenticator()

    # or re-authenticate
    auth = drio.Authenticator(auth_force=True)

.. caution::

    Users on shared computers should always re-authenticate since access token
    from a different user may unintentionally be used.

If you require machine-to-machine/server type of authentication,
:ref:`contact us <support>` and we will provide you the specifics.

Legacy users may authenticate by providing user credential, but
use :py:class:`authenticate.UserCredentials` or 
:py:class:`authenticate.UnsafeUserCredentials`. However, this possibility will
be deprecated in the near feature.


Caching
*******
The :py:class:`Client` class employs a disk cache to speed up repeating series
downloads. Beside turning the cache on and off, several aspects of it can be
configured during instantiation. The configuration are passed on as a
dictionary:

* ``format``: format used to store series on disk, either 'msgpack' or 'csv'. Default is 'msgpack'.
* ``max_size``: size in megabytes that the cache is allowed to use. Default is 1024MB.
* ``cache_root``: control the cache storage location. Default locations are:
    
    * Windows: ``%LOCALAPPDATA%\\datareservoirio\\Cache``
    * Linux: ``~/.cache/datareservoirio`` (XDG default)
    * MacOs: ``~/Library/Caches/datareservoirio``

Example::

    import datareservoirio as drio


    auth = drio.Authenticator()

    # Initiate a client with 32GB cache in the 'c:\project\drio_cache'
    client = drio.Client(auth, cache=True,
                         cache_opt={'format': 'msgpack', 'max_size': 32*1024,
                                    'cache_root': r'c:\project\drio_cache'})

The cache has near disk-bound performance and will benefit greatly from fast
low-latency solid state drives.

.. warning::

    The cache is "cleaned up" during instantiation of :py:class:`Client`. If
    it is instantiated with defaults cache options, it will potentially delete
    the larger cache set up by an another instance! Caution is adviced!

.. note::

    If you are working with several "larger" projects at once, it may be a good
    idea to set up a different cache (storage location) for each project.


Logging
*******

Coming soon, stay tuned!
