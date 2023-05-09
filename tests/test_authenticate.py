import pytest
import os

from datareservoirio.authenticate import TokenCache


class Test_TokenCache:

    @pytest.fixture
    def token_cache(self):
        token_cache = TokenCache()
        return token_cache

    @pytest.mark.parametrize("session_key", [None, "foobar"])
    def test__init__(self, session_key):
        token_cache = TokenCache(session_key=session_key)

        assert os.path.exists(token_cache._token_root)

    def test__token_root(self, token_cache):
        root_out = token_cache._token_root
        assert os.path.exists(root_out)
