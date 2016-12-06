import timeseriesclient
import numpy as np
import pandas as pd
import logging
import sys
import unittest
from timeit import default_timer as timer
from adal.adal_error import AdalError

import timeseriesclient.adalwrapper as adalw

def configure():
    timeseriesclient.set_log_level(logging.DEBUG)
    timeseriesclient.logger.addHandler(logging.StreamHandler(sys.stdout))
    timeseriesclient.globalsettings.environment.set_qa()

def make_test_client():
    client = timeseriesclient.TimeSeriesClient()

    username = 'reservoir-integrationtest@4subsea.onmicrosoft.com'
    password = 'LnqABDrHLYceXLWC7YFhbVAq8dqvPeRAMzbTYKGn'

    client._authenticator = adalw.UnsafeAuthenticator(username, password)

    return client
    
class Test_Authenticate(unittest.TestCase):

    def test_authentication_succeeds_without_error(self):
        client = make_test_client()
        client.authenticate()

    def test_authentication_raises_error(self):
        client = make_test_client()
        client._authenticator.password = 'the wrong password'

        with self.assertRaises(AdalError):
            client.authenticate()

    def test_authentication_token(self):
        client = make_test_client()
        client.authenticate()

        self.assertIsInstance(client.token, dict) 
        
    def test_performance(self):
        client = make_test_client()

        start = timer()
        client.authenticate()
        stop = timer()

        print("login took {} seconds".format(stop-start))
        self.assertLess(stop-start, 0.8)

    def test_refresh_token(self):
        client = make_test_client()
        client.authenticate()

        token1 = client.token
        client._authenticator.refresh_token()
        token2 = client.token

        self.assertNotEqual(token2, token1)
        

class Test_Ping(unittest.TestCase):

    def setUp(self):
        self.client = make_test_client()
        self.client.authenticate()
    
    def test_ping_suceeds(self):
        self.client.ping()

class Test_Upload(unittest.TestCase):

    def setUp(self):
        configure()

    def test_upload(self):
        client = make_test_client()
        client.authenticate()

        df = pd.DataFrame({'a':np.arange(1e6)})

        result = client.create(df)


