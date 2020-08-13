import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock, mock_open, DEFAULT

import pytest

import datareservoirio
from datareservoirio import authenticate
from datareservoirio import _constants


def setUpModule():
    datareservoirio.globalsettings.environment.set_qa()


class TestAuthenticator(unittest.TestCase):

    def test_authenticator_exists(self):
        self.assertTrue(hasattr(datareservoirio, 'Authenticator'))


class TestAccessToken(unittest.TestCase):

    def test_accesstoken_exists(self):
        self.assertTrue(hasattr(authenticate, 'AccessToken'))


class TestBaseAuthSession(unittest.TestCase):

    @classmethod
    @patch('datareservoirio.authenticate.OAuth2Session.__init__')
    def setUpClass(cls, mock_session):
        class MockAuthClass(authenticate.BaseAuthSession):
            """
            Mock-ish class to test abstract class.

            """
            def __init__(self, auth_force=False):
                # self._params = self._get_params(legacy_auth=True)
                session_params = {
                    "client_secret": "victoria's",
                    "token_url": "my_token_url",
                    "authority": "cartman",
                }

                client = Mock()
                super(MockAuthClass, self).__init__(client, session_params, auth_force=auth_force)

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

        assert auth._session_params == {
            "client_secret": "victoria's",
            "authority": "cartman",
            }

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

    @patch('datareservoirio.authenticate.TokenCache')
    def test_exception_is_caught_if_no_cached_token_url(self, mock_cache):
        self.mock_refresh_token.side_effect = ValueError
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
        tc = authenticate.TokenCache()
        self.mock_makedirs.assert_not_called()
        self.assertEqual(tc._session_key, '')

    def test_init_no_token(self):
        self.mock_path_exists.return_value = False
        authenticate.TokenCache()
        self.mock_makedirs.assert_called_once()

    def test_init_without_session_key(self):
        self.mock_path_exists.return_value = False
        dummy_key = None
        tc = authenticate.TokenCache(session_key=dummy_key)
        self.assertEqual(tc._session_key, '')

    def test_init_with_session_key(self):
        self.mock_path_exists.return_value = False
        dummy_key = 'abc'
        tc = authenticate.TokenCache(session_key=dummy_key)
        self.assertEqual(tc._session_key, f'.{dummy_key}')

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

    @patch("datareservoirio.authenticate.json.dump")
    def test_dump(self, mock_json):
        tc = authenticate.TokenCache()
        tc._token_url = "my_token_url"
        m = mock_open()
        with patch('datareservoirio.authenticate.open', m):
            tc.dump({"token": "abc"})

        m.assert_called_once()
        mock_json.assert_called_once_with({"token": "abc", "token_url": "my_token_url"}, m())

    def test_token_file_exist(self):
        m = mock_open(read_data='{"token": "abc", "token_url": "my_token_url"}')
        tc = authenticate.TokenCache()
        with patch('datareservoirio.authenticate.open', m):
            token = tc.token

        assert token == {"token": "abc"}
        assert tc.token_url == "my_token_url"

    def test_token_file_dont_exist(self):
        m = mock_open()
        m.side_effect = FileNotFoundError
        tc = authenticate.TokenCache()
        with patch('datareservoirio.authenticate.open', m):
            token = tc.token

        assert token is None


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

        # fix since base class is mocked
        auth._session_params = {
            "username": "username",
            "resource": _constants.RESOURCE_QA_USERLEGACY,
            "token_url": _constants.TOKEN_URL_USERLEGACY,
        }
        auth._token_url = auth._session_params["token_url"]

        args, kwargs = auth._prepare_fetch_token_args()

        args_expected = (_constants.TOKEN_URL_USERLEGACY, )
        kwargs_exptected = {
            'resource': _constants.RESOURCE_QA_USERLEGACY,
            'username': "username",
            'password': auth._get_pass(),
            'include_client_id': True
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)

    @patch('datareservoirio.authenticate.BaseAuthSession.token')
    def test__prepare_refresh_token_args(self, mock_token):
        auth = authenticate.UserCredentials('username')
                # fix since base class is mocked
        auth._session_params = {
            "username": "username",
            "resource": _constants.RESOURCE_QA_USERLEGACY,
            "token_url": _constants.TOKEN_URL_USERLEGACY,
        }
        auth._token_url = auth._session_params["token_url"]
        mock_token.__getitem__.return_value = '123abc'

        args, kwargs = auth._prepare_refresh_token_args()

        args_expected = (_constants.TOKEN_URL_USERLEGACY, )
        kwargs_exptected = {'refresh_token': '123abc'}

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)


class TestUserAuthenticator(unittest.TestCase):

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
        authenticate.UserAuthenticator()

    def test__prepare_fetch_token_args(self):
        auth = authenticate.UserAuthenticator()
        auth._token_cache = MagicMock()
        auth._session_params = {
            "client_secret": _constants.CLIENT_SECRET_QA_USER,
            "token_url": None,
            "authority": _constants.AUTHORITY_URL_QA_USER,
        }

        args, kwargs = auth._prepare_fetch_token_args()

        args_expected = ('https://token-endpoint.com', )
        kwargs_exptected = {
            'code': 'abc321',
            'client_secret': _constants.CLIENT_SECRET_QA_USER
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)

    @patch('datareservoirio.authenticate.BaseAuthSession.token')
    def test__prepare_refresh_token_args(self, mock_token):
        auth = authenticate.UserAuthenticator()
        auth._token_cache = MagicMock()
        auth._session_params = {
            "client_secret": _constants.CLIENT_SECRET_QA_USER,
            "token_url": None,
            "authority": _constants.AUTHORITY_URL_QA_USER,
        }
        auth._token_url = 'https://token-endpoint.com'

        mock_token.__getitem__.return_value = '123abc'

        args, kwargs = auth._prepare_refresh_token_args()

        args_expected = ('https://token-endpoint.com', )
        kwargs_exptected = {
            'refresh_token': '123abc',
            'client_secret': _constants.CLIENT_SECRET_QA_USER
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)


class TestClientAuthenticator(unittest.TestCase):

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
        authenticate.ClientAuthenticator("my_client_id", "my_client_secret")

    def test__prepare_fetch_token_args(self):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")
        auth._token_cache = MagicMock()
        auth._session_params = {
            "client_secret": "my_client_id",
            "token_url": _constants.TOKEN_URL_QA_CLIENT,
            "scope": _constants.SCOPE_QA_CLIENT,
        }
        auth._token_url = _constants.TOKEN_URL_QA_CLIENT

        args, kwargs = auth._prepare_fetch_token_args()

        args_expected = (_constants.TOKEN_URL_QA_CLIENT, )
        kwargs_exptected = {
            "client_secret": "my_client_id",
            "scope": _constants.SCOPE_QA_CLIENT,
            "include_client_id": True,
        }

        self.assertTupleEqual(args, args_expected)
        self.assertDictEqual(kwargs, kwargs_exptected)

    @patch('datareservoirio.authenticate.BaseAuthSession.token')
    def test__prepare_refresh_token_args(self, mock_token):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")
        auth._token_cache = MagicMock()
        auth._session_params = {
            "client_secret": _constants.CLIENT_SECRET_QA_USER,
            "token_url": None,
            "authority": _constants.AUTHORITY_URL_QA_USER,
        }
        auth._token_url = 'https://token-endpoint.com'
        mock_token.__getitem__.return_value = '123abc'

        assert auth._prepare_refresh_token_args() is None

    def test_refresh_token(self):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")
        auth._token_cache = MagicMock()
        auth._session_params = {
            "client_secret": "my_client_id",
            "token_url": _constants.TOKEN_URL_QA_CLIENT,
            "scope": _constants.SCOPE_QA_CLIENT,
        }
        auth._token_url = _constants.TOKEN_URL_QA_CLIENT

        with pytest.raises(NotImplementedError):
            auth.refresh_token()


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
