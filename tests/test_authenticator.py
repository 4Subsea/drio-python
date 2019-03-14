import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock, mock_open, DEFAULT


import datareservoirio
from datareservoirio import authenticate
from datareservoirio import _constants


def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class TestOAuth2Parameters(unittest.TestCase):

    def test_environment_dev_legacy(self):
        params = authenticate.OAuth2Parameters(_constants.ENV_DEV, legacy_auth=True)

        self.assertEqual(params.resource, _constants.RESOURCE_DEV_LEGACY)
        self.verify_shared_legacy_parameters(params)

    def test_environment_test_legacy(self):
        params = authenticate.OAuth2Parameters(_constants.ENV_TEST, legacy_auth=True)

        self.assertEqual(params.resource, _constants.RESOURCE_TEST_LEGACY)
        self.verify_shared_legacy_parameters(params)

    def test_environment_qa_legacy(self):
        params = authenticate.OAuth2Parameters(_constants.ENV_QA, legacy_auth=True)

        self.assertEqual(params.resource, _constants.RESOURCE_QA_LEGACY)
        self.verify_shared_legacy_parameters(params)

    def test_environment_prod_legacy(self):
        params = authenticate.OAuth2Parameters(_constants.ENV_PROD, legacy_auth=True)

        self.assertEqual(params.resource, _constants.RESOURCE_PROD_LEGACY)
        self.verify_shared_legacy_parameters(params)

    def verify_shared_legacy_parameters(self, params):
        self.assertEqual(params.authority, _constants.AUTHORITY_URL_LEGACY)
        self.assertEqual(params.client_id, _constants.CLIENT_ID_LEGACY)
        self.assertEqual(params.token_url, _constants.TOKEN_URL_LEGACY)

    def test_environment_test(self):
        envs = [_constants.ENV_DEV, _constants.ENV_TEST,
                _constants.ENV_QA, _constants.ENV_PROD]
        for env in envs:
            params = authenticate.OAuth2Parameters(env, legacy_auth=False)
            self.assertEqual(params.authority, getattr(
                _constants, 'AUTHORITY_URL_{}'.format(env)))
            self.assertEqual(params.client_id, getattr(
                _constants, 'CLIENT_ID_{}'.format(env)))
            self.assertEqual(params.client_secret, getattr(
                _constants, 'CLIENT_SECRET_{}'.format(env)))
            self.assertEqual(params.redirect_uri, getattr(
                _constants, 'REDIRECT_URI_{}'.format(env)))
            self.assertEqual(params.token_url, None)
            self.assertEqual(params.scope, getattr(
                _constants, 'SCOPE_{}'.format(env)))


class TestAuthenticator(unittest.TestCase):

    def test_authenticator_exists(self):
        self.assertTrue(hasattr(datareservoirio, 'Authenticator'))


class TestBaseAuthSession(unittest.TestCase):

    @classmethod
    @patch('datareservoirio.authenticate.OAuth2Session.__init__')
    def setUpClass(cls, mock_session):
        class MockAuthClass(authenticate.BaseAuthSession):
            """
            Mock-ish class to test abstract class.

            """
            def __init__(self, auth_force=False):
                self._params = self._get_params(legacy_auth=True)
                client = Mock()
                super(MockAuthClass, self).__init__(client, auth_force=auth_force)

            def _prepare_fetch_token_args(self):
                args = ('token_url', )
                kwargs = {
                    'client_id': 'python-client',
                    'resource': 'tiberium',
                    'username': 'bruce_wayne',
                    'password': 'batman'
                }
                return args, kwargs

            def _prepare_refresh_token_args(self):
                args = ('token_url', )
                kwargs = {'refresh_token': self.token['refresh_token']}
                return args, kwargs

            def _fetch_token_initial(self):
                pass

        cls.MochAuthClass = MockAuthClass

    def setUp(self):
        self._fetch_token_patcher = patch(
            'datareservoirio.authenticate.OAuth2Session.fetch_token')
        self._refresh_token_patcher = patch(
            'datareservoirio.authenticate.OAuth2Session.refresh_token')
        self._fetch_token_initial_patcher = patch.object(
            self.MochAuthClass, '_fetch_token_initial')

        self.mock_fetch_token = self._fetch_token_patcher.start()
        self.mock_refresh_token = self._refresh_token_patcher.start()
        self.mock_fetch_initial_token = self._fetch_token_initial_patcher.start()

        self.addCleanup(self._fetch_token_patcher.stop)
        self.addCleanup(self._refresh_token_patcher.stop)
        self.addCleanup(self._fetch_token_initial_patcher.stop)

    def tearDown(self):
        pass

    @patch('datareservoirio.authenticate.TokenCache')
    @patch('datareservoirio.authenticate.BaseAuthSession.fetch_token')
    def test_init(self, mock_cache, mock_token):
        mock_cache.load.return_value = None

        auth = self.MochAuthClass()
        self.assertIsInstance(auth._params, authenticate.OAuth2Parameters)

    @patch('datareservoirio.authenticate.TokenCache')
    def test_fetch_token(self, mock_cache):
        mock_cache.load.return_value = None

        with patch('datareservoirio.authenticate.BaseAuthSession.fetch_token') as mock_fetch:
            auth = self.MochAuthClass()
        auth.fetch_token()
        self.mock_fetch_token.assert_called_once()

    @patch('datareservoirio.authenticate.TokenCache')
    def test_refresh_token(self, mock_cache):
        mock_cache.load.return_value = None

        with patch('datareservoirio.authenticate.BaseAuthSession.fetch_token') as mock_fetch:
            auth = self.MochAuthClass()
            token = {
                'access_token': '123abc',
                'refresh_token': '456def'}
            auth.token = token
        auth.refresh_token()

        self.mock_refresh_token.assert_called()

    @patch('datareservoirio.authenticate.TokenCache')
    def test_auth_force(self, mock_cache):
        self.MochAuthClass(auth_force=True)

        self.mock_fetch_initial_token.assert_called_once()

    @patch('datareservoirio.authenticate.TokenCache')
    def test_exception_is_caught_if_no_cache(self, mock_cache):
        self.mock_refresh_token.side_effect = KeyError
        self.MochAuthClass(auth_force=False)

        self.mock_fetch_initial_token.assert_called_once()


class TestTokenCache(unittest.TestCase):
    def setUp(self):
        self._patcher_makedirs = patch(
            'datareservoirio.authenticate.os.makedirs')
        self.mock_makedirs = self._patcher_makedirs.start()

        self._patcher_path_exists = patch(
            'datareservoirio.authenticate.os.path.exists')
        self.mock_path_exists = self._patcher_path_exists.start()

        self.addCleanup(self._patcher_makedirs.stop)
        self.addCleanup(self._patcher_path_exists.stop)

    def tearDown(self):
        pass

    def test_init_existing_token(self):
        self.mock_path_exists.return_value = True
        authenticate.TokenCache()
        self.mock_makedirs.assert_not_called()

    def test_init_no_token(self):
        self.mock_path_exists.return_value = False
        authenticate.TokenCache()
        self.mock_makedirs.assert_called_once()

    def test_call(self):
        with patch('datareservoirio.authenticate.TokenCache.dump') as mock_dump:
            dummy_input = 'abcd'
            tc = authenticate.TokenCache()
            tc(dummy_input)
        mock_dump.assert_called_once_with(dummy_input)

    @patch('datareservoirio.authenticate.user_data_dir', return_value='a\\random\\path')
    @patch('datareservoirio.authenticate.environment.get', return_value='pc')
    def test_token_path(self, mock_env, mock_user_data_dir):
        tc = authenticate.TokenCache()

        expected = 'a\\random\\path\\token.pc'
        path = tc.token_path
        self.assertEqual(expected, path)

    @patch('datareservoirio.authenticate.json.dumps', return_value='dumped_data')
    @patch('datareservoirio.authenticate.open')
    def test_dump(self, mock_open, mock_json_dumps):
        dummy_token = {'token': 'dummy'}
        tc = authenticate.TokenCache()

        with patch.multiple(tc, _scrambler=DEFAULT, _token_url='token_url') as mock_values:
            tc.dump(dummy_token)

        mock_json_dumps.assert_called_once_with(dummy_token)
        mock_values['_scrambler'].encrypt.assert_called_once_with(b'dumped_data')

    @patch('datareservoirio.authenticate.json.loads', return_value={'dummy': 'token'})
    def test_load_file_exist(self, mock_json_loads):
        with patch('datareservoirio.authenticate.open', mock_open(read_data=b'scrambled_token')):
            tc = authenticate.TokenCache()
            tc._scrambler = MagicMock()
            tc.load()
            tc._scrambler.decrypt.assert_called_once_with(b'scrambled_token')
            mock_json_loads.assert_called_once()

    @patch('datareservoirio.authenticate.json.loads', return_value='token')
    @patch('datareservoirio.authenticate.TokenCache.token_path', return_value='path\\token')
    @patch('datareservoirio.authenticate.TokenCache.__init__', return_value=None)
    def test_load_no_file_exist(self, mock_init, mock_token_path, mock_json_loads):
        mock_open_read = mock_open(read_data=b'scrambled_token')
        mock_open_read.side_effect = FileNotFoundError
        with patch('datareservoirio.authenticate.open', mock_open_read):
            tc = authenticate.TokenCache()
            ret_val = tc.load()
            self.assertIsNone(ret_val)
            mock_json_loads.assert_not_called()

    @patch('datareservoirio.authenticate.Fernet')
    @patch('datareservoirio.authenticate.urlsafe_b64encode', return_value='key')
    def test_scrambler_init(self, mock_b64encode, mock_fernet):
        authenticate.TokenCache()
        mock_b64encode.assert_called_once()
        mock_fernet.assert_called_once_with('key')


class TestUserCredentials(unittest.TestCase):

    def setUp(self):
        self._patcher = patch(
            'datareservoirio.authenticate.BaseAuthSession.__init__')
        self.mock_init = self._patcher.start()

        self._patcher_get_pass = patch(
            'datareservoirio.authenticate.UserCredentials._get_pass')
        self.mock_get_pass = self._patcher_get_pass.start()

        self.mock_get_pass.return_value = 'passwd'

        datareservoirio.globalsettings.environment.set_qa()

        self.addCleanup(self._patcher.stop)
        self.addCleanup(self._patcher_get_pass.stop)

    def tearDown(self):
        pass

    def test_init(self):
        authenticate.UserCredentials('username')

    def test__prepare_fetch_token_args(self):
        auth = authenticate.UserCredentials('username')
        auth.username = 'username'  # fix since base class is mocked
        args, kwargs = auth._prepare_fetch_token_args()

        params = authenticate.OAuth2Parameters('QA', legacy_auth=True)
        args_expected = (params.token_url, )
        kwargs_exptected = {
            'client_id': params.client_id,
            'resource': params.resource,
            'username': auth.username,
            'password': auth._get_pass(),
            'include_client_id': True
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)

    @patch('datareservoirio.authenticate.BaseAuthSession.token')
    def test__prepare_refresh_token_args(self, mock_token):
        auth = authenticate.UserCredentials('username')
        auth.username = 'username'  # fix since base class is mocked
        mock_token.__getitem__.return_value = '123abc'

        args, kwargs = auth._prepare_refresh_token_args()

        params = authenticate.OAuth2Parameters('QA', legacy_auth=True)
        args_expected = (params.token_url, )
        kwargs_exptected = {'refresh_token': '123abc'}

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)


class TestAccessToken(unittest.TestCase):

    def setUp(self):
        self._patcher = patch(
            'datareservoirio.authenticate.BaseAuthSession.__init__')
        self.mock_init = self._patcher.start()

        self._patcher_input = patch(
            'datareservoirio.authenticate.input', return_value='{"endpoint":"https://token-endpoint.com","code":"abc321"}')
        self.mock_input = self._patcher_input.start()

        datareservoirio.globalsettings.environment.set_qa()

        self.addCleanup(self._patcher.stop)
        self.addCleanup(self._patcher_input.stop)

    def tearDown(self):
        pass

    def test_init(self):
        authenticate.AccessToken()

    def test__prepare_fetch_token_args(self):
        auth = authenticate.AccessToken()
        auth._token_cache = MagicMock()

        args, kwargs = auth._prepare_fetch_token_args()

        params = authenticate.OAuth2Parameters('QA', legacy_auth=False)
        args_expected = ('https://token-endpoint.com', )
        kwargs_exptected = {
            'code': 'abc321',
            'client_secret': params.client_secret
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)

    @patch('datareservoirio.authenticate.BaseAuthSession.token')
    def test__prepare_refresh_token_args(self, mock_token):
        auth = authenticate.AccessToken()
        auth._token_cache = MagicMock()
        auth._params.token_url = 'https://token-endpoint.com'

        mock_token.__getitem__.return_value = '123abc'

        args, kwargs = auth._prepare_refresh_token_args()

        params = authenticate.OAuth2Parameters('QA', legacy_auth=False)
        args_expected = ('https://token-endpoint.com', )
        kwargs_exptected = {
            'refresh_token': '123abc',
            'client_secret': _constants.CLIENT_SECRET_QA
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)


class Test_UnsafeUserCredentials(unittest.TestCase):

    def setUp(self):
        self._patcher = patch(
            'datareservoirio.authenticate.BaseAuthSession.__init__')
        self.mock_get_token = self._patcher.start()

        self.addCleanup(self._patcher.stop)

    def tearDown(self):
        pass

    def test_init(self):
        auth = authenticate.UnsafeUserCredentials('username', 'secret_password')
        self.assertEqual(auth._password, 'secret_password')

    def test__get_pass(self):
        auth = authenticate.UnsafeUserCredentials('username', 'secret_password')
        password = auth._get_pass()
        self.assertEqual(password, 'secret_password')


if __name__ == '__main__':
    unittest.main()
