import pytest
import os
import shutil
from pathlib import Path

from datareservoirio.authenticate import TokenCache


TEST_PATH = Path(__file__).parent


class Test_TokenCache:

    @pytest.fixture
    def token_root(self, tmp_path):
        return tmp_path / "datareservoirio"

    @pytest.fixture
    def copytokens(self, token_root):
        """Copy tokens from source to ``token_root``"""
        src = TEST_PATH / "testdata" / "tokens"
        token_root.mkdir(exist_ok=True)
        for file_i in src.iterdir():
            shutil.copyfile(file_i, token_root / file_i.name)

    @pytest.fixture(autouse=True)
    def mock_user_data_dir(self, monkeypatch, tmp_path):
        def mock_user_data_dir(appname, *args, **kwargs):
            return str(tmp_path / appname)

        monkeypatch.setattr("datareservoirio.authenticate.user_data_dir", mock_user_data_dir)

    @pytest.fixture
    def token_cache(self):
        """TokenCache instance (without token)"""
        token_cache = TokenCache()
        return token_cache

    @pytest.fixture
    def token_cache2(self, copytokens):
        """TokenCache instance (with token)"""
        token_cache = TokenCache()
        return token_cache

    @pytest.mark.parametrize("session_key", [None, "foobar"])
    def test__init__(self, session_key):
        token_cache = TokenCache(session_key=session_key)

        assert os.path.exists(token_cache._token_root)

    def test__token_root(self, token_cache, tmp_path):
        root_out = token_cache._token_root
        assert os.path.exists(root_out)
        assert os.path.split(root_out)[-1] == "datareservoirio"
        assert root_out == str(tmp_path / "datareservoirio")

    def test_token_path(self, token_cache):
        path_out = token_cache.token_path
        path_expect = os.path.join(token_cache._token_root, "token.PROD")
        assert path_out == path_expect

    def test_token_path_with_session_key(self):
        token_cache = TokenCache(session_key="foobar")
        path_out = token_cache.token_path
        path_expect = os.path.join(token_cache._token_root, "token.PROD.foobar")
        assert path_out == path_expect

    def test_token_none(self, token_cache):
        token_out = token_cache.token
        assert token_out is None

    def test_token_exists(self, token_cache2):
        token_out = token_cache2.token
        assert token_out is not None

    def test_token_exists_session_key(self, copytokens):
        token_cache = TokenCache(session_key="foobar")
        token_out = token_cache.token
        assert token_out is not None
