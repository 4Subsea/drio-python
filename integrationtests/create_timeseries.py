import timeseriesclient
import numpy as np
import pandas as pd
import logging
import sys
import unittest
from timeit import default_timer as timer
from adal.adal_error import AdalError

import timeseriesclient.adalwrapper as adalw
from timeseriesclient.log import LogWriter
from utils import make_test_client, configure
        
logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

class Test_Ping(unittest.TestCase):

    def setUp(self):
        self.client = make_test_client()
        self.client.authenticate()
    
    def test_ping_suceeds(self):
        self.client.ping()

class Test_Upload(unittest.TestCase):

    def setUp(self):
        configure()

    def test_upload_int64(self):
        client = make_test_client()
        client.authenticate()

        df = pd.DataFrame({'a':np.arange(1e6, dtype=np.int64)})
        result = client.create(df)

        logwriter.info("Got result, checking it...")
        self.assertIsInstance(result, dict)
        self.assertTrue("TimeSeriesId" in result.keys())


    def test_upload_datetime64(self):
        client = make_test_client()
        client.authenticate()

        index = np.array(np.arange(1e8, 10e15, 10e9, dtype=np.int64), dtype='datetime64[ns]')
        df = pd.DataFrame({'a':np.arange(1e6)}, index=index)
        result = client.create(df)

        logwriter.info("Got result, checking it...")
        self.assertIsInstance(result, dict)
        self.assertTrue("TimeSeriesId" in result.keys())

        


