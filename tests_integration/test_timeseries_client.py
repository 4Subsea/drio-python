import pprint
from timeit import default_timer as timer
import unittest
from mock import patch

import numpy as np
import pandas as pd

import timeseriesclient
from timeseriesclient.authenticate import Authenticator

timeseriesclient.globalsettings.environment.set_test()

USERNAME = 'ace@4subsea.com'
PASSWORD = '#bmE378dt!'

#USERNAME = 'reservoir-integrationtest@4subsea.onmicrosoft.com'
#PASSWORD = 'LnqABDrHLYceXLWC7YFhbVAq8dqvPeRAMzbTYKGn'


class Test_TimeSeriesApi(unittest.TestCase):

    @classmethod
    @patch('getpass.getpass', return_value=PASSWORD)
    def setUpClass(cls, mock_input):
        cls.auth = Authenticator(USERNAME)

        cls.df_1 = pd.DataFrame({'values': np.arange(100.)}, index=np.arange(0, 100))
        cls.df_1.index.name = 'time'
        cls.df_2 = pd.DataFrame({'values': np.arange(100.)}, index=np.arange(50, 150))
        cls.df_2.index.name = 'time'
        cls.df_3 = pd.DataFrame({'values': np.arange(50.)}, index=np.arange(125, 175))
        cls.df_3.index.name = 'time'

    def setUp(self):
        self.client = timeseriesclient.TimeSeriesClient(self.auth)

    def test_ping(self):
        self.client.ping()

    def test_create_get_delete(self):
        response = self.client.create(self.df_1)
        info = self.client.info(response['TimeSeriesId'])

        pprint.pprint(info)

        self.assertEqual(0, response['TimeOfFirstSample'])
        self.assertEqual(info['TimeOfFirstSample'], response['TimeOfFirstSample'])

        self.assertEqual(99, response['TimeOfLastSample'])
        self.assertEqual(info['TimeOfLastSample'], response['TimeOfLastSample'])

        df_1_recieved = self.client.get(response['TimeSeriesId'])

        pd.util.testing.assert_series_equal(self.df_1['values'], df_1_recieved)

        self.client.delete(response['TimeSeriesId'])

    def test_list(self):
        response = self.client.list()
        self.assertTrue(isinstance(response, list))

    def test_delete(self):
        response = self.client.create(self.df_3)
        info_pre = self.client.info(response['TimeSeriesId'])
        self.client.delete(response['TimeSeriesId'])

        with self.assertRaises(ValueError):
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

        pd.util.testing.assert_series_equal(data_sent['values'], data_recieved)

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

        pd.util.testing.assert_series_equal(data_sent['values'], data_recieved)

    def test_create_get_performance(self):
        # 1 day @ 10Hz
        df = pd.DataFrame({'values': np.arange(864000.)}, index=np.arange(0, 864000))
        df.index.name = 'time'

        start = timer()
        response = self.client.create(df)
        stop = timer()
        print('Upload too slow: {}'.format(stop-start))

#        self.assertLessEqual(stop-start, 1000., msg='Upload too slow: {}'.format(stop-start))
        info = self.client.info(response['TimeSeriesId'])

        start = timer()
        self.client.get(response['TimeSeriesId'])
        stop = timer()
        print('Download too slow: {}'.format(stop-start))

#        self.assertLessEqual(stop-start, 1000., msg='Download too slow: {}'.format(stop-start))

        pprint.pprint(info)
        self.client.delete(response['TimeSeriesId'])


if __name__ == '__main__':
    unittest.main()
