import numpy as np
import pandas as pd
import logging
import sys
import unittest
from timeit import default_timer as timer
from adal.adal_error import AdalError

import timeseriesclient
import timeseriesclient.adalwrapper as adalw
from utils import make_test_client, configure

configure()

    
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
        self.assertLess(stop-start, 1.0)

    def test_refresh_token(self):
        client = make_test_client()
        client.authenticate()

        token1 = client.token
        client._authenticator.refresh_token()
        token2 = client.token

        self.assertNotEqual(token2, token1)
