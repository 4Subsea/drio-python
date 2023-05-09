import pytest

from datareservoirio.environments import Environment


class Test_Environment:

    @pytest.fixture
    def environment(self):
        environment = Environment()
        return environment

    def test__init__(self):
        environment = Environment()
        assert environment.current_environment == "PROD"
        assert environment.api_base_url == "https://reservoir-api.4subsea.net/api/"

    def test_api_base_url(self, environment):
        base_url_out = environment.api_base_url
        base_url_expect = "https://reservoir-api.4subsea.net/api/"
        assert base_url_out == base_url_expect

    def test_current_environment(self, environment):
        current_env_out = environment.current_environment
        current_env_expect = "PROD"
        assert current_env_out == current_env_expect
