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

try:
    from .utils import make_test_client, configure
except:
    from utils import make_test_client, configure
        
logger = logging.getLogger('timeseriesclient.integrationtests')
logwriter = LogWriter(logger)

def chunk_dataframe(dataframe, chunksize):
    i = 0
    j = chunksize

    while i<len(dataframe):
        yield(dataframe.iloc[i:j])
        i+=chunksize
        j+=chunksize

class Test_Upload(unittest.TestCase):

    def setUp(self):
        configure()

    def test_create_append_list_info_delete(self):
        client = make_test_client()
        client.authenticate()

        df = pd.DataFrame({'a':np.arange(1e6)})

        timeseries_id = None

        for chunk in chunk_dataframe(df, int(3e5)):
            if timeseries_id == None:
                logwriter.debug("creating new timeseries")
                response = client.create(chunk)
                timeseries_id = response['TimeSeriesId']
            else:
                logwriter.debug("appending a chunk")
                response = client.add(chunk, timeseries_id)
                logwriter.debug(response)
                
        self.assertIsInstance(response, dict)
        self.assertTrue("TimeSeriesId" in response.keys())

        # Next Step: check if created timeseries is listed
        logwriter.info("Verifying that timeseries {} is listed".format(timeseries_id))
        available_timeseries_dict = client.list()
        available_timeseries = [x["TimeSeriesId"] for x in available_timeseries_dict]
        self.assertIn(timeseries_id, available_timeseries)

        # Next Step: info
        logwriter.info("Getting information about timeseries {}".format(timeseries_id))
        info = client.info(timeseries_id)
        self.assertIsInstance(info, dict)
        self.assertEqual(info["TimeSeriesId"], timeseries_id)

        # Next Step: delete the timeseries
        logwriter.info("Deleting timeseries {}".format(timeseries_id))
        client.delete(timeseries_id)

        # Next Step: verify that the timeseries is not listed anymore
        logwriter.info("Verifying that timeseries {} is *NOT* listed".format(timeseries_id))
        available_timeseries_dict = client.list()
        available_timeseries = [x["TimeSeriesId"] for x in available_timeseries_dict]
        self.assertNotIn(timeseries_id, available_timeseries)

        



