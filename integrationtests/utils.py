import logging
import sys

import timeseriesclient
import timeseriesclient.adalwrapper as adalw

def configure():
    timeseriesclient.set_log_level(logging.DEBUG)
    
    for hdlr in timeseriesclient.logger.handlers:
        timeseriesclient.logger.removeHandler(hdlr)

    timeseriesclient.logger.addHandler(logging.StreamHandler(sys.stdout))
    timeseriesclient.globalsettings.environment.set_qa()

def make_test_client():
    client = timeseriesclient.TimeSeriesClient()

    username = 'reservoir-integrationtest@4subsea.onmicrosoft.com'
    password = 'LnqABDrHLYceXLWC7YFhbVAq8dqvPeRAMzbTYKGn'

    client._authenticator = adalw.UnsafeAuthenticator(username, password)

    return client
