import pprint
import logging
import unittest
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import requests

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


class Test_Client(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

        cls.df_1 = pd.Series(np.arange(100.), index=np.arange(0, 100))
        cls.df_2 = pd.Series(np.arange(100.), index=np.arange(50, 150))
        cls.df_3 = pd.Series(np.arange(50.), index=np.arange(125, 175))

    def setUp(self):
        self.client = datareservoirio.Client(self.auth, cache=False)

    def tearDown(self):
        self.client.__exit__()

    def test_ping(self):
        self.client.ping()

    def test_create_empty_series_get_then_delete(self):
        response = self.client.create()
        ts_id = response['TimeSeriesId']

        # self.assertEqual(0, response['TimeOfFirstSample'])
        # self.assertEqual(info['TimeOfFirstSample'],
        #                  response['TimeOfFirstSample'])

        # self.assertEqual(99, response['TimeOfLastSample'])
        # self.assertEqual(info['TimeOfLastSample'],
        #                  response['TimeOfLastSample'])

        df_recieved = self.client.get(ts_id, convert_date=True)

        self.client.delete(ts_id)

        self.assertEqual(0, len(df_recieved.index))

    def test_create_get_delete(self):
        rng = pd.date_range('1970-01-01', periods=100, freq='ns')
        df = pd.Series(np.arange(100.), index=rng)
        response = self.client.create(df)
        info = self.client.info(response['TimeSeriesId'])

        self.assertEqual(0, response['TimeOfFirstSample'])
        self.assertEqual(info['TimeOfFirstSample'],
                         response['TimeOfFirstSample'])

        self.assertEqual(99, response['TimeOfLastSample'])
        self.assertEqual(info['TimeOfLastSample'],
                         response['TimeOfLastSample'])

        df_recieved = self.client.get(
            response['TimeSeriesId'], convert_date=True)

        pd.util.testing.assert_series_equal(df, df_recieved)

        self.client.delete(response['TimeSeriesId'])

    def test_delete(self):
        response = self.client.create(self.df_3)
        ts_id = response['TimeSeriesId']
        self.client.delete(ts_id)

        with self.assertRaises(requests.exceptions.HTTPError):
            self.client.info(ts_id)

    def test_create_append_nooverlap_get_delete(self):
        response = self.client.create(self.df_1)

        self.client.append(self.df_3, response['TimeSeriesId'])

        info = self.client.info(response['TimeSeriesId'])
        pprint.pprint(info)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_recieved = self.client.get(info['TimeSeriesId'], convert_date=False)
        data_sent = self.df_1
        data_sent = data_sent.append(self.df_3)

        pd.util.testing.assert_series_equal(data_sent, data_recieved)

    def test_create_append_overlap_get_delete(self):
        response = self.client.create(self.df_1)

        self.client.append(self.df_2, response['TimeSeriesId'])
        self.client.append(self.df_3, response['TimeSeriesId'])

        info = self.client.info(response['TimeSeriesId'])
        pprint.pprint(info)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_recieved = self.client.get(info['TimeSeriesId'], convert_date=False)
        data_sent = self.df_3
        data_sent = data_sent.combine_first(self.df_2)
        data_sent = data_sent.combine_first(self.df_1)

        pd.util.testing.assert_series_equal(data_sent, data_recieved)

    def test_create_get_performance(self):
        # 10 days @ 10Hz
        df = pd.Series(np.arange(10 * 864000.),
                       index=np.arange(0, 10 * 864000))

        start = timer()
        response = self.client.create(df)
        stop = timer()
        print('Average upload time per day: {}'.format((stop - start) / 10.))

        info = self.client.info(response['TimeSeriesId'])

        start = timer()
        self.client.get(response['TimeSeriesId'])
        stop = timer()
        print('Average download time per day: {}'.format((stop - start) / 10.))

        pprint.pprint(info)
        self.client.delete(response['TimeSeriesId'])


class Test_Client_CacheEnable(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

        cls.df_1 = pd.Series(np.arange(100.), index=np.arange(0, 100))
        cls.df_2 = pd.Series(np.arange(100.), index=np.arange(50, 150))
        cls.df_3 = pd.Series(np.arange(50.), index=np.arange(125, 175))

    def setUp(self):
        self.client = datareservoirio.Client(self.auth, cache=True)

    def tearDown(self):
        self.client.__exit__()

    def test_ping(self):
        self.client.ping()

    def test_create_get_delete(self):
        rng = pd.date_range('1970-01-01', periods=100, freq='ns')
        df = pd.Series(np.arange(100.), index=rng)
        response = self.client.create(df)
        info = self.client.info(response['TimeSeriesId'])

        self.assertEqual(0, response['TimeOfFirstSample'])
        self.assertEqual(info['TimeOfFirstSample'],
                         response['TimeOfFirstSample'])

        self.assertEqual(99, response['TimeOfLastSample'])
        self.assertEqual(info['TimeOfLastSample'],
                         response['TimeOfLastSample'])

        df_recieved = self.client.get(
            response['TimeSeriesId'], convert_date=True)

        pd.util.testing.assert_series_equal(df, df_recieved)

        self.client.delete(response['TimeSeriesId'])

    def test_delete(self):
        response = self.client.create(self.df_3)

        self.client.delete(response['TimeSeriesId'])

        with self.assertRaises(requests.exceptions.HTTPError):
            self.client.info(response['TimeSeriesId'])

    def test_create_append_nooverlap_get_delete(self):
        response = self.client.create(self.df_1)

        self.client.append(self.df_3, response['TimeSeriesId'])

        info = self.client.info(response['TimeSeriesId'])
        pprint.pprint(info)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_recieved = self.client.get(info['TimeSeriesId'], convert_date=False)
        data_sent = self.df_1
        data_sent = data_sent.append(self.df_3)

        pd.util.testing.assert_series_equal(data_sent, data_recieved)

    def test_create_append_overlap_get_delete(self):
        response = self.client.create(self.df_1)

        self.client.append(self.df_2, response['TimeSeriesId'])
        self.client.append(self.df_3, response['TimeSeriesId'])

        info = self.client.info(response['TimeSeriesId'])
        pprint.pprint(info)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_recieved = self.client.get(info['TimeSeriesId'], convert_date=False)
        data_sent = self.df_3
        data_sent = data_sent.combine_first(self.df_2)
        data_sent = data_sent.combine_first(self.df_1)

        pd.util.testing.assert_series_equal(data_sent, data_recieved)

    def test_create_get_performance(self):
        # 10 days @ 10Hz
        df = pd.Series(np.arange(10 * 864000.),
                       index=np.arange(0, 10 * 864000))

        start = timer()
        response = self.client.create(df)
        stop = timer()
        print('Average upload time per day: {}'.format((stop - start) / 10.))

        info = self.client.info(response['TimeSeriesId'])

        start = timer()
        self.client.get(response['TimeSeriesId'])
        stop = timer()
        print('Average download time per day: {}'.format((stop - start) / 10.))

        pprint.pprint(info)
        self.client.delete(response['TimeSeriesId'])


if __name__ == '__main__':
    logger = logging.getLogger("datareservoirio")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    unittest.main()
