Release Notes
=============

v0.10.2
-------

- fix: remove rouge print statement

v0.10.1
-------

- fix: missing submodules

v0.10.0
-------

- update azure-storage-blob to latest
- fix: add support for blob versioning
- other improvements and fixes

v0.9.8
------

- unpin pandas and pyarrow versions


v0.9.6
------

- Cache clearing did not correctly index chunks that have multiple versions for 
the same day. This might occur for streaming timeseries were the latest data is 
continuously pulled from DRIO

v0.9.5
------

- Fix - Case 24768 - Refine logging in cache_engine (#7)

* Rely on the support for formatting time and level into the message in the logging framework
* Additional debug logging for write, commit and evict operations.
* Log delete errors using logging.error instead of warnings.warn, since this is potentially not something that the application can handle
* Delete the LogWriter wrapper and change implementations to call directly on loggers
* Include blob names in download progress logging
* Include timing in cache eviction logging

v0.9.3
------

Case 24071: Allow file process status monitoring to be skipped in create and append (#6)

* Allow file process status monitoring to be skipped in create and append
* Updated doc with example
* Renamed parameter to wait_on_verification

v0.1.0
------

- First release
