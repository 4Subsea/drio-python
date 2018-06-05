import unittest

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock

from datareservoirio.rest_api.base import (
    BaseAPI, TokenAuth, _update_kwargs)


class Test_BaseAPI(unittest.TestCase):

    def setUp(self):
        self._session = Mock()
        self.api = BaseAPI(session=self._session)

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


if __name__ == '__main__':
    unittest.main()
