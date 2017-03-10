import datetime
import unittest

import numpy as np

import timeseriesclient
import timeseriesclient.authenticate as adalw
import timeseriesclient._constants as consts

try:
    from unittest.mock import patch, Mock
except:
    from mock import patch, Mock


def setUpModule():
    timeseriesclient.globalsettings.environment.set_qa()


class TestADALParameters(unittest.TestCase):

    def test_test_environment(self):
        params = adalw.ADALParameters(consts.ENV_TEST)

        self.assertEqual(params.resource, consts.RESOURCE_TEST)
        self.verify_shared_parameters(params)

    def test_qa_environment(self):
        params = adalw.ADALParameters(consts.ENV_QA)

        self.assertEqual(params.resource, consts.RESOURCE_QA)
        self.verify_shared_parameters(params)

    def test_prod_environment(self):
        params = adalw.ADALParameters(consts.ENV_PROD)

        self.assertEqual(params.resource, consts.RESOURCE_PROD)
        self.verify_shared_parameters(params)

    def verify_shared_parameters(self, params):
        self.assertEqual(params.client_id, consts.CLIENT_ID)
        self.assertEqual(params.authority, consts.AUTHORITY)


class TestAuthenticator(unittest.TestCase):

    def test_authenticator_exists(self):
        self.assertTrue(hasattr(timeseriesclient, 'Authenticator'))


class TestAdalAuthenticator(unittest.TestCase):

    def setUp(self):
        self._patcher = patch(
            'timeseriesclient.authenticate.adal.AuthenticationContext.acquire_token_with_username_password')
        self.mock_get_token = self._patcher.start()

        self._patcher_get_pass = patch(
            'timeseriesclient.authenticate.AdalAuthenticator._get_pass')
        self.mock_get_pass = self._patcher_get_pass.start()

        self.mock_get_pass.return_value = 'passwd'

    def tearDown(self):
        self._patcher.stop()
        self._patcher_get_pass.stop()

    @patch('timeseriesclient.authenticate.adal.AuthenticationContext')
    def test_authenticate_calls_authorizationContext_with_correct_authority(self, mock):
        authenticator = adalw.AdalAuthenticator('username')
        mock.assert_called_once_with(consts.AUTHORITY, api_version=None)

    def test_authenticate_calls_authorize_method_with_correct_params(self):
        authenticator = adalw.AdalAuthenticator('username_01')

        self.assertEqual(
            timeseriesclient.globalsettings.environment.get(), consts.ENV_QA)

        self.mock_get_token.assert_called_once_with(consts.RESOURCE_QA,
                                                    'username_01',
                                                    'passwd',
                                                    consts.CLIENT_ID)

    @patch('timeseriesclient.authenticate.AdalAuthenticator._get_pass')
    def test_init_calls_get_pass(self, mock_get_pass):

        authenticator = adalw.AdalAuthenticator('username')
        mock_get_pass.assert_called_once()

    @patch('timeseriesclient.authenticate.adal.AuthenticationContext.acquire_token')
    def test_token(self, mock_acquire_token):
        dummy_token = {'accessToken': 'abcdef',
                       'expiresOn': np.datetime64('2050-01-01 00:00:00', 's')}
        mock_acquire_token.return_value = dummy_token
        authenticator = adalw.AdalAuthenticator('username')
        token = authenticator.token
        self.assertEqual(token, dummy_token)


class Test_UnsafeAdalAuthenticator(unittest.TestCase):

    @patch('timeseriesclient.authenticate.AdalAuthenticator.__init__')
    def test_init(self, mock_auth):
        username = 'username_A3'
        password = 'passwd'
        authenticator = adalw.UnsafeAdalAuthenticator(username, password)
        mock_auth.assert_called_once_with(username)

    @patch('timeseriesclient.authenticate.AdalAuthenticator.__init__')
    def test_get_pass(self, mock_auth):
        username = 'username_A3'
        password = 'passwd'
        authenticator = adalw.UnsafeAdalAuthenticator(username, password)
        self.assertEqual(authenticator._get_pass(), 'passwd')


if __name__ == '__main__':
    unittest.main()
