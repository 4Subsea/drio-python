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


class TestAdalWrapper(unittest.TestCase):

    def test_authenticator_exists(self):
        authenticator = adalw.Authenticator()


class TestAdalWrapperAuthenticate(unittest.TestCase):

    def setUp(self):
        self._patcher = patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_username_password')
        self.mock_get_token = self._patcher.start()
        
        self._patcher_get_user_creds = patch('timeseriesclient.adalwrapper.usercredentials.get_user_credentials')
        self.mock_get_user_creds = self._patcher_get_user_creds.start()

        self.mock_get_user_creds.return_value = ('username', 'passwd')

    def tearDown(self):
        self._patcher.stop()
        self._patcher_get_user_creds.stop()

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext')
    def test_authenticate_calls_authorizationContext_with_correct_authority(self, mock):
        authenticator = adalw.Authenticator()
        authenticator.authenticate()

        mock.assert_called_once_with(consts.AUTHORITY, api_version=None)

    def test_authenticate_calls_authorize_method_with_correct_params(self):
        authenticator = adalw.Authenticator()
        authenticator.authenticate()

        self.assertEqual(timeseriesclient.globalsettings.environment.get(), consts.ENV_QA)


        self.mock_get_token.assert_called_once_with(consts.RESOURCE_QA, 
                                                    'username', 
                                                    'passwd', 
                                                    consts.CLIENT_ID)

    def test_authenticate_calls_get_user_credentials(self):
        
        authenticator = adalw.Authenticator()
        authenticator.authenticate()

        self.mock_get_user_creds.assert_called_with()

    def test_token_init(self):
        authenticator = adalw.Authenticator()

        self.assertIsNone(authenticator._token)

    def test_token(self):
        authenticator = adalw.Authenticator()

        dummy_token = { 'accessToken' : 'abcdef', 
                        'expiresOn' : np.datetime64('2050-01-01 00:00:00', 's') }
        self.mock_get_token.return_value = dummy_token

        authenticator.authenticate()

        token = authenticator.token

        self.assertEqual(token, dummy_token)


class TestAdalWrapperRefreshToken(unittest.TestCase):
    
    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_username_password')
    def test_(self, mock):
        auth = adalw.Authenticator()

    def test_calls_authenticate_if_token_is_None(self):
        auth = adalw.Authenticator()
        auth.authenticate = Mock(return_value={'dummy', 'token'})
        auth._time_until_token_expires = Mock(return_value=10*60+1)
        auth.token

        auth.authenticate.assert_called_with()

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_refresh_token')
    def test_sets_refreshed_token_as_new_token(self, mock):
        auth = adalw.Authenticator()
        auth._token = { 'expiresOn' : '2016-12-06 12:00:00', 'refreshToken':'asdf'}
        mock.return_value = { 'expiresOn' : 'newtime'}
        auth._time_until_token_expires = Mock(return_value=10*60-1)

        token = auth.token
    
        self.assertEqual(token, { 'expiresOn' : 'newtime'})

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_refresh_token')
    def test_calls_time_till_token_expires(self, mock):
        auth = adalw.Authenticator()
        auth._token = { 'expiresOn' : '2016-12-06 12:00:00', 'refreshToken':'asdf'}
        mock.return_value = { 'expiresOn' : 'newtime'}

        auth._time_until_token_expires = Mock(return_value = 0)
        auth.token
        auth._time_until_token_expires.assert_called_with()

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_refresh_token')
    def test_calls_refresh_token_if_time_left_les_than_limit(self, mock):
        auth = adalw.Authenticator()
        auth._token = { 'expiresOn' : '2016-12-06 12:00:00', 'refreshToken':'asdf'}
        mock.return_value = { 'expiresOn' : 'newtime'}
        
        auth._time_until_token_expires = Mock(return_value=10*60-1)
        auth.refresh_token = Mock()

        auth.token

        auth.refresh_token.assert_called_with()

    def test_time_until_token_expires(self):
        auth = adalw.Authenticator()
        auth._get_utcnow = Mock(return_value=np.datetime64('2016-12-06 12:00:00', 's'))

        auth._token = { 'expiresOn' : '2016-12-06 12:00:00'}
        self.assertEqual(auth._time_until_token_expires(), 0)

        auth._token = { 'expiresOn' : '2016-12-06 12:01:00'}
        self.assertEqual(auth._time_until_token_expires(), 60)
    
        auth._token = { 'expiresOn' : '2016-12-06 11:59:00'}
        self.assertEqual(auth._time_until_token_expires(), -60)

    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_refresh_token')
    def test_refresh_token(self, mock_refresh):
        pass


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


class Test_UnsafeAuthenticator(unittest.TestCase):
    
    @patch('timeseriesclient.adalwrapper.adal.AuthenticationContext.acquire_token_with_username_password')
    def test_(self, mock_get_token):
        username='uname'
        password='passwd'
        authenticator = adalw.UnsafeAuthenticator(username, password)

        authenticator.authenticate()

        mock_get_token.assert_called_once_with(consts.RESOURCE_QA, 
                                                    username, 
                                                    password, 
                                                    consts.CLIENT_ID)
