import unittest

try:
    from unittest.mock import  patch, PropertyMock
except:
    from mock import patch, PropertyMock

from azure.storage.blob import BlockBlobService

import timeseriesclient
from timeseriesclient.adalwrapper import Authenticator
import timeseriesclient.globalsettings as gs
import timeseriesclient.storage as storage


class Test_GetBlockBlobService(unittest.TestCase):

    def setUp(self):
        self.upload_params = { 'Account' : 'accountname',
                               'SasKey'  : 'abcdef' }

    @patch('timeseriesclient.storage.BlockBlobService')
    def test_calls_BlockBlobService_constructor_correct(self, mock):
        blobservice = storage.get_blobservice(self.upload_params)

        mock.assert_called_with('accountname', sas_token='abcdef')

    def test_returns_BlockBlobService(self):
        blobservice = storage.get_blobservice(self.upload_params)

        self.assertIsInstance(blobservice, BlockBlobService)
