# Integration tests for datareservoirio

## How to use?
In order to run the integration tests, you must first set the ``DRIO_CLIENT_ID`` and ``DRIO_CLIENT_SECRET``
environment variables. This can be done with the following commands in the CMD prompt window (note that these GUIDs are only for demonstration purposes):

```console
set DRIO_CLIENT_ID=63b8d619-84ac-456d-8a76-bb8fd889e04c
set DRIO_CLIENT_SECRET=63b8d619-84ac-456d-8a76-bb8fd889e04c
```

After having set the environment variables, you can run the integration tests with ``pytest``:

```console
pytest integration_tests
```

