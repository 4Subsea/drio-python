import unittest

from mock import patch

from timeseriesclient.apiwebbase import WebBaseApi


class Test_WebBaseApi(unittest.TestCase):

    def setUp(self):
        self.api = WebBaseApi()

    @patch('timeseriesclient.apiwebbase.requests.post')
    def test__post(self, mock_post):
        self.api._post(1, 2, a='a', b='c')
        mock_post.assert_called_once_with(1, 2, a='a', b='c')

    @patch('timeseriesclient.apiwebbase.requests.get')
    def test__get(self, mock_get):
        self.api._get(1, 2, a='a', b='c')
        mock_get.assert_called_once_with(1, 2, a='a', b='c')

    @patch('timeseriesclient.apiwebbase.requests.delete')
    def test__delete(self, mock_delete):
        self.api._delete(1, 2, a='a', b='c')
        mock_delete.assert_called_once_with(1, 2, a='a', b='c')
