import json
from unittest.mock import MagicMock, call, patch

import pytest

import datareservoirio as drio
from datareservoirio import _constants, authenticate
from datareservoirio.globalsettings import environment


def setup_module():
    environment.set_test()


@patch("datareservoirio.authenticate.user_data_dir")
class Test_TokenCache:
    def test_init_dir_doesnt_exists(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir

        assert not cache_dir.exists()
        authenticate.TokenCache()
        assert cache_dir.exists()

    def test_init_dir_exists(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        assert cache_dir.exists()
        token_cache = authenticate.TokenCache()

        assert token_cache._token == {}

    def test_init_cache_exists(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        cache_path = cache_dir / f"token.{environment.get()}"

        with open(cache_path, "w") as f:
            json.dump({"access_token": "123abc"}, f)

        token_cache = authenticate.TokenCache()
        assert token_cache._token == {"access_token": "123abc"}

    def test__token_root(self, mock_cache_dir):
        mock_cache_dir.return_value = "my_dir"
        token_cache = authenticate.TokenCache()
        token_root = token_cache._token_root

        assert token_root == "my_dir"
        mock_cache_dir.assert_called_with("datareservoirio")

    def test_token_path(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        cache_path = cache_dir / f"token.{environment.get()}"

        token_cache = authenticate.TokenCache()
        assert token_cache.token_path == str(cache_path)

    def test_token_path_session_key(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        session_key = "my_session_key"
        cache_path = cache_dir / f"token.{environment.get()}.{session_key}"

        token_cache = authenticate.TokenCache(session_key=session_key)
        assert token_cache.token_path == str(cache_path)

    def test_token(self, mock_cache_dir):
        mock_cache_dir.return_value = "my_dir"
        token_cache = authenticate.TokenCache()
        token_cache._token = {"access_token": "123abc"}

        assert token_cache.token == {"access_token": "123abc"}

    def test_token_none(self, mock_cache_dir):
        mock_cache_dir.return_value = "my_dir"
        token_cache = authenticate.TokenCache()

        assert token_cache.token is None

    def test_append(self, mock_cache_dir):
        mock_cache_dir.return_value = "my_dir"
        token_cache = authenticate.TokenCache()
        token_cache._token = {"access_token": "123abc"}

        token_cache.append("new_key", "new_value")

        assert token_cache.token == {"access_token": "123abc", "new_key": "new_value"}

    def test_dump_session_key(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        session_key = "my_session_key"
        cache_path = cache_dir / f"token.{environment.get()}.{session_key}"

        token_cache = authenticate.TokenCache(session_key=session_key)

        token = {"access_token": "123abc"}
        token_cache.dump(token)

        with open(cache_path, "r") as f:
            token_output = json.load(f)

        assert token == token_output

    def test_dump(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        cache_path = cache_dir / f"token.{environment.get()}"

        token_cache = authenticate.TokenCache()

        token = {"access_token": "123abc"}
        token_cache.dump(token)

        with open(cache_path, "r") as f:
            token_output = json.load(f)

        assert token == token_output

    def test_dump_append(self, mock_cache_dir, tmp_path):
        cache_dir = tmp_path / "cache_dir"
        mock_cache_dir.return_value = cache_dir
        cache_dir.mkdir()

        cache_path = cache_dir / f"token.{environment.get()}"

        token_cache = authenticate.TokenCache()
        token_cache.append("new_key", "new_value")

        token = {"access_token": "123abc"}
        token_cache.dump(token)

        with open(cache_path, "r") as f:
            token_output = json.load(f)

        token.update({"new_key": "new_value"})
        assert token == token_output

    def test_call(self, mock_cache_dir):
        mock_cache_dir.return_value = "my_dir"
        token_cache = authenticate.TokenCache()

        with patch.object(token_cache, "dump") as mock_dump:
            token_cache({"access_token": "123abc"})

        mock_dump.assert_called_once_with({"access_token": "123abc"})


@patch("datareservoirio.authenticate.OAuth2Session.refresh_token")
@patch("datareservoirio.authenticate.OAuth2Session.fetch_token")
class Test_ClientAutheticator:
    def test_init(self, mock_fetch, mock_refresh):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")

        mock_fetch.assert_called_once_with(
            _constants.TOKEN_URL_TEST_CLIENT,
            client_secret="my_client_secret",
            scope=_constants.SCOPE_TEST_CLIENT,
            include_client_id=True,
        )
        assert auth.auto_refresh_url == _constants.TOKEN_URL_TEST_CLIENT
        assert (
            auth.headers["user-agent"] == f"python-datareservoirio/{drio.__version__}"
        )

    def test_refresh_token(self, mock_fetch, mock_refresh):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")
        auth.refresh_token()
        mock_fetch.assert_called()

    def test_prepare_refresh_token_args(self, mock_fetch, mock_refresh):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")

        assert auth._prepare_refresh_token_args() is None

    def test_prepare_fetch_token_args(self, mock_fetch, mock_refresh):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")

        args, kwargs = auth._prepare_fetch_token_args()

        args_expected = (_constants.TOKEN_URL_TEST_CLIENT,)
        kwargs_expected = {
            "client_secret": "my_client_secret",
            "scope": _constants.SCOPE_TEST_CLIENT,
            "include_client_id": True,
        }

        assert args == args_expected
        assert kwargs == kwargs_expected

    def test_fetch_token(self, mock_fetch, mock_refresh):
        auth = authenticate.ClientAuthenticator("my_client_id", "my_client_secret")

        auth.fetch_token()

        mock_fetch.assert_called_with(
            _constants.TOKEN_URL_TEST_CLIENT,
            client_secret="my_client_secret",
            scope=_constants.SCOPE_TEST_CLIENT,
            include_client_id=True,
        )


@patch("datareservoirio.authenticate.OAuth2Session.refresh_token")
@patch("datareservoirio.authenticate.OAuth2Session.fetch_token")
@patch("datareservoirio.authenticate.TokenCache")
class Test_UserAuthenticator:
    def test_init_force_auth(self, mock_token, mock_fetch, mock_refresh):
        with patch.object(
            authenticate.UserAuthenticator, "_prepare_fetch_token_args"
        ) as mock_fetch_args:
            mock_fetch_args.return_value = (
                ("my_token_url",),
                {
                    "code": "my_code",
                    "client_secret": _constants.CLIENT_SECRET_TEST_USER,
                },
            )

            auth = authenticate.UserAuthenticator(auth_force=True)

            mock_fetch.assert_called_once_with(
                "my_token_url",
                code="my_code",
                client_secret=_constants.CLIENT_SECRET_TEST_USER,
            )
        mock_token.assert_called()
        auth.token_updater.assert_called()
        assert (
            auth.headers["user-agent"] == f"python-datareservoirio/{drio.__version__}"
        )

    def test_init_session_key(self, mock_token, mock_fetch, mock_refresh):
        with patch.object(
            authenticate.UserAuthenticator, "_prepare_fetch_token_args"
        ) as mock_fetch_args:
            mock_fetch_args.return_value = (
                ("my_token_url",),
                {
                    "code": "my_code",
                    "client_secret": _constants.CLIENT_SECRET_TEST_USER,
                },
            )

            authenticate.UserAuthenticator(auth_force=True, session_key="test")
            mock_token.assert_called_once_with(session_key="test")

    def test_init_force_auth_false_token_none(
        self, mock_token, mock_fetch, mock_refresh
    ):
        with patch.object(
            authenticate.UserAuthenticator, "_prepare_fetch_token_args"
        ) as mock_fetch_args:
            mock_fetch_args.return_value = (
                ("my_token_url",),
                {
                    "code": "my_code",
                    "client_secret": _constants.CLIENT_SECRET_TEST_USER,
                },
            )

            token_cache = MagicMock()
            token_cache.token = None
            mock_token.return_value = token_cache

            authenticate.UserAuthenticator(auth_force=False)

            mock_fetch.assert_called_once_with(
                "my_token_url",
                code="my_code",
                client_secret=_constants.CLIENT_SECRET_TEST_USER,
            )

    def test_init_force_auth_false_token_valid(
        self, mock_token, mock_fetch, mock_refresh
    ):
        token_cache = MagicMock()
        token_cache.token = {
            "access_token": "my_access_token",
            "refresh_token": "my_refresh_token",
            "token_url": "my_token_url",
        }
        mock_token.return_value = token_cache

        auth = authenticate.UserAuthenticator(auth_force=False)

        mock_refresh.assert_called_once_with(
            "my_token_url",
            refresh_token="my_refresh_token",
            client_secret=_constants.CLIENT_SECRET_TEST_USER,
        )

        assert auth.token == token_cache.token
        assert auth.auto_refresh_url == "my_token_url"

    def test__prepare_refresh_token_args(self, mock_token, mock_fetch, mock_refresh):
        """Assume valid token in cache"""
        token_cache = MagicMock()
        token_cache.token = {
            "access_token": "my_access_token",
            "refresh_token": "my_refresh_token",
            "token_url": "my_token_url",
        }
        mock_token.return_value = token_cache

        auth = authenticate.UserAuthenticator(auth_force=False)

        args, kwargs = auth._prepare_refresh_token_args()
        assert args == ("my_token_url",)
        assert kwargs == {
            "refresh_token": "my_refresh_token",
            "client_secret": _constants.CLIENT_SECRET_TEST_USER,
        }

    def test_refesh_token(self, mock_token, mock_fetch, mock_refresh):
        """Assume valid token in cache"""
        token_cache = MagicMock()
        token_cache.token = {
            "access_token": "my_access_token",
            "refresh_token": "my_refresh_token",
            "token_url": "my_token_url",
        }
        mock_token.return_value = token_cache

        auth = authenticate.UserAuthenticator(auth_force=False)
        auth.refresh_token()

        mock_refresh.assert_called_with(
            "my_token_url",
            refresh_token="my_refresh_token",
            client_secret=_constants.CLIENT_SECRET_TEST_USER,
        )
        assert mock_refresh.call_count == 2

    def test_fetch_token(self, mock_token, mock_fetch, mock_refresh):
        with patch.object(
            authenticate.UserAuthenticator, "_prepare_fetch_token_args"
        ) as mock_fetch_args:
            mock_fetch_args.return_value = (
                ("my_token_url",),
                {
                    "code": "my_code",
                    "client_secret": _constants.CLIENT_SECRET_TEST_USER,
                },
            )

            auth = authenticate.UserAuthenticator(auth_force=True)
            auth.fetch_token()

        mock_fetch.assert_called_with(
            "my_token_url",
            code="my_code",
            client_secret=_constants.CLIENT_SECRET_TEST_USER,
        )
        assert mock_fetch.call_count == 2

    @patch("datareservoirio.authenticate.input")
    def test_prepare_fetch_token_args(
        self, mock_input, mock_token, mock_fetch, mock_refresh
    ):
        token_cache = MagicMock()
        token_cache.token = None
        mock_token.return_value = token_cache

        mock_input.return_value = (
            '{"endpoint":"https://token-endpoint.com","code":"abc321"}'
        )

        auth = authenticate.UserAuthenticator(auth_force=True)
        args, kwargs = auth._prepare_fetch_token_args()

        assert args == ("https://token-endpoint.com",)
        assert kwargs == {
            "code": "abc321",
            "client_secret": _constants.CLIENT_SECRET_TEST_USER,
        }


if __name__ == "__main__":
    pytest.main()
