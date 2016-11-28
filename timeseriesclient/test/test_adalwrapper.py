import unittest

try:
    from unittest.mock import patch
except:
    from mock import patch

import sys
sys.path.append('../../')

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

        dummy_token = { 'accessToken' : 'abcdef' }
        self.mock_get_token.return_value = dummy_token

        authenticator.authenticate()

        token = authenticator.token

        self.assertEqual(token, dummy_token)

class TestAdalWrapperRefreshToken(unittest.TestCase):
    

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





