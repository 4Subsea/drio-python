import unittest
import datetime

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock

import numpy as np

import timeseriesclient
import timeseriesclient.adalwrapper as adalw
import timeseriesclient.constants as consts

timeseriesclient.globalsettings.environment.set_qa()


class TestAuthenticator(unittest.TestCase):

    def test_authenticator_exists(self):
        self.assertTrue(hasattr(timeseriesclient, 'Authenticator'))


class TestAdalAuthenticator(unittest.TestCase):

    def setUp(self):
        self._patcher = patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_username_password')
        self.mock_get_token = self._patcher.start()
        
        self._patcher_get_pass = patch('timeseriesclient.adalwrapper.AdalAuthenticator._get_pass')
        self.mock_get_pass = self._patcher_get_pass.start()

        self.mock_get_pass.return_value = 'passwd'

    def tearDown(self):
        self._patcher.stop()
        self._patcher_get_pass.stop()

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext')
    def test_authenticate_calls_authorizationContext_with_correct_authority(self, mock):
        authenticator = adalw.AdalAuthenticator('username')
        mock.assert_called_once_with(consts.AUTHORITY, api_version=None)

    def test_authenticate_calls_authorize_method_with_correct_params(self):
        authenticator = adalw.AdalAuthenticator('username_01')

        self.assertEqual(timeseriesclient.globalsettings.environment.get(), consts.ENV_QA)

        self.mock_get_token.assert_called_once_with(consts.RESOURCE_QA, 
                                                    'username_01', 
                                                    'passwd', 
                                                    consts.CLIENT_ID)

    @patch('timeseriesclient.adalwrapper.AdalAuthenticator._get_pass')
    def test_init_calls_get_pass(self, mock_get_pass):
        
        authenticator = adalw.AdalAuthenticator('username')
        mock_get_pass.assert_called_once()

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token')
    def test_token(self, mock_acquire_token):
        dummy_token = { 'accessToken' : 'abcdef', 
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        mock_acquire_token.return_value = dummy_token
        authenticator = adalw.AdalAuthenticator('username')
        token = authenticator.token
        self.assertEqual(token, dummy_token)


class Test_HeaderFunctions(unittest.TestCase):

    def test_create_authorization_header(self):
        token = {'accessToken' : 'abcdef'}

        key, value = adalw.create_authorization_header(token)

        self.assertEqual(key, 'Authorization')
        self.assertEqual(value, 'Bearer abcdef')

    def test_add_authorization_header(self):
        token = {'accessToken' : 'abcdef'}
        key = 'Authorization'
        value = 'Bearer abcdef'
        expected_header = { key : value }
       
        header = adalw.add_authorization_header({}, token)

        self.assertEqual(header, expected_header)


class Test_UnsafeAdalAuthenticator(unittest.TestCase):

    @patch('timeseriesclient.adalwrapper.AdalAuthenticator.__init__')
    def test_init(self, mock_auth):
        username='username_A3'
        password='passwd'
        authenticator = adalw.UnsafeAdalAuthenticator(username, password)
        mock_auth.assert_called_once_with(username)

    @patch('timeseriesclient.adalwrapper.AdalAuthenticator.__init__')
    def test_get_pass(self, mock_auth):
        username='username_A3'
        password='passwd'
        authenticator = adalw.UnsafeAdalAuthenticator(username, password)
        self.assertEqual(authenticator._get_pass(), 'passwd')
