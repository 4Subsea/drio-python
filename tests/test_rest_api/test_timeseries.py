import time
import unittest
import requests

import datareservoirio
from datareservoirio.rest_api import TimeSeriesAPI
from datareservoirio.rest_api.timeseries import (
    request_cache,
    _make_request_hash)

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock


dummy_response_add = """
    {
      "FileId": 0,
      "TimeOfFirstSample": 0,
      "TimeOfLastSample": 0,
      "TimeSeriesId": 0,
      "FileStatus": 0,
      "ReferenceTime": "2016-12-01T11:18:57.481Z",
      "LastModifiedByEmail": "string",
      "Created": "2016-12-01T11:18:57.481Z",
      "LastModified": "2016-12-01T11:18:57.481Z",
      "CreatedByEmail": "string"
    }"""


def make_add_response():
    response = requests.Response()
    response._content = dummy_response_add.encode('ascii')

    return response


def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class Test_TimeSeriesAPI(unittest.TestCase):

    def setUp(self):
        self.token = {'accessToken': 'abcdef'}

        self._session = requests.Session()
        self.api = TimeSeriesAPI(session=self._session)
        self.api._session = Mock()
        self.api._root = 'https://root/timeseries/'

    def tearDown(self):
        self._session.close()

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_info(self, mock_token):
        mock_get = self.api._session.get

        mock_get.return_value = Mock()
        mock_get.return_value.text = u'{}'

        self.api.info(self.token, "someId")

        expected_uri = 'https://root/timeseries/someId'

        mock_get.assert_called_once_with(expected_uri, auth=mock_token(),
                                         **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_delete(self, mock_token):
        mock_delete = self.api._session.delete
        timeseries_id = '123456'

        mock_delete.return_value = Mock()
        mock_delete.return_value.status_code = 200

        self.api.delete(self.token, timeseries_id)

        expected_uri = 'https://root/timeseries/123456'

        mock_delete.assert_called_with(expected_uri, auth=mock_token(),
                                       **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_create_with_timeseries_id(self, mock_token):
        ts_id = 'ebaebc1e-35f6-49b5-a6cf-3cc07177a691'

        mock_put = self.api._session.put
        mock_put.return_value = Mock()
        mock_put.return_value.text = u'{TimeSeriesId:\'ebaebc1e-35f6-49b5-a6cf-3cc07177a691\'}'

        self.api.create(self.token, timeseries_id=ts_id)

        expected_uri = 'https://root/timeseries/ebaebc1e-35f6-49b5-a6cf-3cc07177a691'

        mock_put.assert_called_with(expected_uri, data=None, auth=mock_token(), **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.uuid4')
    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_create_without_timeseries_id(self, mock_token, mock_uuid):
        mock_uuid.return_value = u'aaabbbcc-35f6-49b5-a6cf-3cc07177a691'
        mock_put = self.api._session.put
        mock_put.return_value = Mock()
        mock_put.return_value.text = u'{TimeSeriesId:\'aaabbbcc-35f6-49b5-a6cf-3cc07177a691\'}'

        self.api.create(self.token)

        expected_uri = 'https://root/timeseries/aaabbbcc-35f6-49b5-a6cf-3cc07177a691'

        mock_put.assert_called_with(expected_uri, data=None, auth=mock_token(), **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_create_with_file(self, mock_token):
        file_id = 666

        mock_post = self.api._session.post
        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        self.api.create_with_data(self.token, file_id)

        expected_uri = 'https://root/timeseries/create'
        expected_body = {"FileId": file_id}

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     data=expected_body, **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_add(self, mock_token):
        timeseries_id = 't666'
        file_id = 'f001'

        mock_post = self.api._session.post
        mock_post.return_value = Mock()
        mock_post.return_value.text = u'{}'

        self.api.add(self.token, timeseries_id, file_id)

        expected_uri = 'https://root/timeseries/add'
        expected_body = {"TimeSeriesId": timeseries_id, "FileId": file_id}

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     data=expected_body, **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test__download_days_base(self, mock_token):
        timeseries_id = 't666'
        start = -1000
        end = 6660000

        self.api._download_days_base(self.token, timeseries_id, start, end)

        expected_uri = 'https://root/timeseries/{ts_id}/data/days'.format(
            ts_id=timeseries_id)
        expected_params = {'start': start, 'end': end}

        mock_get = self.api._session.get
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    params=expected_params,
                                    **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test__download_days_cached(self, mock_token):
        timeseries_id = 't666'
        start = -1000
        end = 6660000

        self.api._download_days_cached(self.token, timeseries_id, start, end)

        expected_uri = 'https://root/timeseries/{ts_id}/data/days'.format(
            ts_id=timeseries_id)
        expected_params = {'start': start, 'end': end}

        mock_get = self.api._session.get
        mock_get.assert_called_with(expected_uri, auth=mock_token(),
                                    params=expected_params,
                                    **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_download_days(self, mock_token):
        timeseries_id = 't666'
        nanoseconds_day = 86400000000000
        start = 10*nanoseconds_day
        end = 21*nanoseconds_day - 1

        with patch.object(self.api, '_download_days_cached') as mock_download_days:
            mock_download_days.return_value = Mock()
            result = self.api.download_days(self.token, timeseries_id,
                                            start+102, end-102933)
        mock_download_days.assert_called_once_with(self.token, timeseries_id,
                                                   start, end)
        self.assertEqual(result, mock_download_days.return_value)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_download_days_exact_end(self, mock_token):
        timeseries_id = 't666'
        nanoseconds_day = 86400000000000
        start = 10*nanoseconds_day
        end = 21*nanoseconds_day
        end_expected = 22*nanoseconds_day - 1

        with patch.object(self.api, '_download_days_cached') as mock_download_days:
            mock_download_days.return_value = Mock()
            result = self.api.download_days(self.token, timeseries_id,
                                            start, end)
        mock_download_days.assert_called_once_with(self.token, timeseries_id,
                                                   start, end_expected)
        self.assertEqual(result, mock_download_days.return_value)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_attach_metadata(self, mock_token):
        timeseries_id = 't666'
        meta_list = ['meta_1', 'meta_2']
        mock_post = self.api._session.put

        self.api.attach_metadata(self.token, timeseries_id, meta_list)

        expected_uri = 'https://root/timeseries/{}/metadata'.format(timeseries_id)

        mock_post.assert_called_with(expected_uri, auth=mock_token(),
                                     json=meta_list, **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_detach_metadata(self, mock_token):
        timeseries_id = 't666'
        meta_list = ['meta_1', 'meta_2']
        mock_delete = self.api._session.delete

        self.api.detach_metadata(self.token, timeseries_id, meta_list)

        expected_uri = 'https://root/timeseries/{}/metadata'.format(timeseries_id)

        mock_delete.assert_called_with(expected_uri, auth=mock_token(),
                                       json=meta_list, **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_search_without_value(self, mock_token):
        mock_get = self.api._session.get

        self.api.search(self.token, 'tns', 'tkey', 'tname', None)

        expected_uri = 'https://root/timeseries/search/tns/tkey/tname'

        mock_get.assert_called_once_with(expected_uri,
                                         auth=mock_token(),
                                         **self.api._defaults)

    @patch('datareservoirio.rest_api.timeseries.TokenAuth')
    def test_search_with_value(self, mock_token):
        mock_get = self.api._session.get

        self.api.search(self.token, 'tns', 'tkey', 'tname', 'theValue')

        expected_uri = 'https://root/timeseries/search/tns/tkey/tname/theValue'

        mock_get.assert_called_once_with(expected_uri,
                                         auth=mock_token(),
                                         **self.api._defaults)


class Test__make_request_hash(unittest.TestCase):
    def test_just_args(self):
        args = (1, 2)
        kwargs = {}

        hash_out = _make_request_hash(args, kwargs)
        self.assertEqual(hash_out, hash((1, 2)))

    def test_just_kwargs(self):
        args = tuple()
        kwargs = {'a': 1}

        hash_out = _make_request_hash(args, kwargs)
        self.assertEqual(hash_out, hash(('a', 1)))

    def test_args_kwargs(self):
        args = (1, 2)
        kwargs = {'a': 1}

        hash_out = _make_request_hash(args, kwargs)
        self.assertEqual(hash_out, hash((1, 2, 'a', 1)))


class Test__request_cache(unittest.TestCase):
    def setUp(self):

        @request_cache()
        def user_function(some_object, somedict, x, y=5):
            return x + y

        self.user_function = user_function

    def test_max_cache_size(self):
        for i in range(512):
            self.user_function(self, {'abc': 123}, 3, i)
        self.assertEqual(len(self.user_function._cache), 256)

    def test_check_cache(self):
        self.user_function(self, {'abc': 123}, 3, 5)

        values = list(self.user_function._cache.values())
        self.assertEqual(len(self.user_function._cache), 1)
        self.assertEqual(values[0][0], 8)

    def test_check_cache_expire(self):
        @request_cache(timeout=1)
        def user_function(some_object, somedict, x, y=5):
            return x + y

        user_function(self, {'abc': 123}, 3, 5)
        self.assertEqual(len(user_function._cache), 1)
        timestamp_0 = list(user_function._cache.values())[0][1]

        time.sleep(2)

        user_function(self, {'abc': 123}, 3, 5)
        self.assertEqual(len(user_function._cache), 1)
        timestamp_1 = list(user_function._cache.values())[0][1]

        self.assertGreater(timestamp_1, timestamp_0)


if __name__ == '__main__':
    unittest.main()
