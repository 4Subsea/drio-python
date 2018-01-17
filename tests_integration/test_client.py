import pprint
import logging
import sys
import unittest
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import requests
from mock import patch

import datareservoirio
from datareservoirio.authenticate import Authenticator

from tests_integration._auth import USER

datareservoirio.globalsettings.environment.set_test()


class Test_TimeSeriesApi(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USER.NAME)

        cls.df_1 = pd.Series(np.arange(100.), index=np.arange(0, 100))
        cls.df_1.index.name = 'index'
        cls.df_1.name = 'values'
        cls.df_2 = pd.Series(np.arange(100.), index=np.arange(50, 150))
        cls.df_2.index.name = 'index'
        cls.df_2.name = 'values'
        cls.df_3 = pd.Series(np.arange(50.), index=np.arange(125, 175))
        cls.df_3.index.name = 'index'
        cls.df_3.name = 'values'

    def setUp(self):
        self.client = datareservoirio.Client(self.auth, cache={'format':'csv', 'max_size': 100.})

    def test_ping(self):
        self.client.ping()

    def test_create_get_delete(self):
        response = self.client.create(self.df_1)
        info = self.client.info(response['TimeSeriesId'])

        pprint.pprint(info)

        self.assertEqual(0, response['TimeOfFirstSample'])
        self.assertEqual(info['TimeOfFirstSample'],
                         response['TimeOfFirstSample'])

        self.assertEqual(99, response['TimeOfLastSample'])
        self.assertEqual(info['TimeOfLastSample'],
                         response['TimeOfLastSample'])

        df_1_recieved = self.client.get(response['TimeSeriesId'])

        pd.util.testing.assert_series_equal(self.df_1['values'], df_1_recieved)

        self.client.delete(response['TimeSeriesId'])

    def test_create_get_delete(self):
        rng = pd.date_range('1970-01-01', periods=100, freq='ns')
        df = pd.Series(np.arange(100.), index=rng)
        df.index.name = 'index'
        df.name = 'values'
        response = self.client.create(df)
        info = self.client.info(response['TimeSeriesId'])

        pprint.pprint(info)

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

    def test_list(self):
        response = self.client.list()
        self.assertTrue(isinstance(response, list))

    def test_delete(self):
        response = self.client.create(self.df_3)
        info_pre = self.client.info(response['TimeSeriesId'])
        self.client.delete(response['TimeSeriesId'])

        with self.assertRaises(requests.exceptions.HTTPError):
            info_post = self.client.info(response['TimeSeriesId'])

    def test_create_append_nooverlap_get_delete(self):
        response = self.client.create(self.df_1)

        self.client.append(self.df_3, response['TimeSeriesId'])

        info = self.client.info(response['TimeSeriesId'])
        pprint.pprint(info)

        self.assertEqual(0, info['TimeOfFirstSample'])
        self.assertEqual(174, info['TimeOfLastSample'])

        data_recieved = self.client.get(info['TimeSeriesId'])
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

        data_recieved = self.client.get(info['TimeSeriesId'])
        data_sent = self.df_3
        data_sent = data_sent.combine_first(self.df_2)
        data_sent = data_sent.combine_first(self.df_1)

        pd.util.testing.assert_series_equal(data_sent, data_recieved)

    def test_create_get_performance(self):
        # 10 days @ 10Hz
        df = pd.Series(np.arange(10 * 864000.),
                       index=np.arange(0, 10 * 864000))
        df.index.name = 'index'
        df.name = 'values'

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
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger("datareservoirio.storage").setLevel(logging.DEBUG)
    unittest.main()
