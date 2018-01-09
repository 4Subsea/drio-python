import unittest
import time

from mock import Mock, patch

from datareservoirio.rest_api.base import (
    BaseAPI, TokenAuth, request_cache, _make_request_hash, _update_kwargs)


class Test_BaseAPI(unittest.TestCase):

    def setUp(self):
        self.api = BaseAPI()
        self.api._session = Mock()

    def test__post(self):
        mock_post = self.api._session.post
        self.api._post(1, 2, a='a', b='c')
        mock_post.assert_called_once_with(1, 2, a='a', b='c',
                                          **self.api._defaults)

    def test__put(self):
        mock_put = self.api._session.put
        self.api._put(1, 2, a='a', b='c')
        mock_put.assert_called_once_with(1, 2, a='a', b='c',
                                         **self.api._defaults)

    def test__get(self):
        mock_get = self.api._session.get
        self.api._get(1, 2, a='a', b='c')
        mock_get.assert_called_once_with(1, 2, a='a', b='c',
                                         **self.api._defaults)

    def test__delete(self):
        mock_delete = self.api._session.delete
        self.api._delete(1, 2, a='a', b='c')
        mock_delete.assert_called_once_with(1, 2, a='a', b='c',
                                            **self.api._defaults)

    def test__update_kwargs(self):
        kwargs = {"abc": 123}
        defaults = {"def": 456}
        _update_kwargs(kwargs, defaults)

        self.assertDictEqual(kwargs, {"abc": 123, "def": 456})


class Test_TokenAuth(unittest.TestCase):

    def setUp(self):
        self.token = {'accessToken': 'abc'}

    def test_init(self):
        self.token_auth = TokenAuth(self.token)
        self.assertDictEqual(self.token_auth.token, self.token)

    def test_call(self):
        r = Mock()
        self.token_auth = TokenAuth(self.token)

        auth_dict = {'Authorization': 'Bearer {}'
                     .format(self.token['accessToken'])}

        r = self.token_auth(r)
        r.headers.update.assert_called_once_with(auth_dict)
        r.headers = auth_dict
        self.assertDictEqual(r.headers, auth_dict)


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
        self.assertEqual(len(self.user_function._cache), 1)
        self.assertEqual(self.user_function._cache.values()[0][0], 8)

    def test_check_cache_expire(self):
        @request_cache(timeout=1)
        def user_function(some_object, somedict, x, y=5):
            return x + y

        user_function(self, {'abc': 123}, 3, 5)
        self.assertEqual(len(user_function._cache), 1)
        timestamp_0 = user_function._cache.values()[0][1]

        time.sleep(2)

        user_function(self, {'abc': 123}, 3, 5)
        self.assertEqual(len(user_function._cache), 1)
        timestamp_1 = user_function._cache.values()[0][1]

        self.assertGreater(timestamp_1, timestamp_0)


if __name__ == '__main__':
    unittest.main()
