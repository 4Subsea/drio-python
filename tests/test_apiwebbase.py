import unittest

from mock import patch

from timeseriesclient.rest_api.base_api import BaseApi


class Test_WebBaseApi(unittest.TestCase):

    def setUp(self):
        self.api = BaseApi()

    @patch('timeseriesclient.rest_api.base_api.requests.post')
    def test__post(self, mock_post):
        self.api._post(1, 2, a='a', b='c')
        mock_post.assert_called_once_with(1, 2, a='a', b='c')

    @patch('timeseriesclient.rest_api.base_api.requests.get')
    def test__get(self, mock_get):
        self.api._get(1, 2, a='a', b='c')
        mock_get.assert_called_once_with(1, 2, a='a', b='c')

    @patch('timeseriesclient.rest_api.base_api.requests.delete')
    def test__delete(self, mock_delete):
        self.api._delete(1, 2, a='a', b='c')
        mock_delete.assert_called_once_with(1, 2, a='a', b='c')


if __name__ == '__main__':
    unittest.main()
