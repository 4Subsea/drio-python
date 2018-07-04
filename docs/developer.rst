Developer
=========

Environments
###########
DataReservoir.io have ``Dev``, ``Test``, ``QA`` and ``Production`` environments.

#. Dev is the local environment on a DataReservoir.io developer machine.
#. Test is dedicated to always have the latest bits. Expect this environment to be potentially unstable.
#. QA is used for new, complete features and approval testing of these features.
#. Production is for... production.


To redirect the DataReservoir client to either of these senvironment, use the following
code in the start of your script.
NOTE: the environment must be selected before creating a ``datareservoirio.Authenticator`` instance.

Example:

    import datareservoirio

    datareservoirio.globalsettings.environment.set_dev()
    datareservoirio.globalsettings.environment.set_test()
    datareservoirio.globalsettings.environment.set_qa()
    datareservoirio.globalsettings.environment.set_production()



