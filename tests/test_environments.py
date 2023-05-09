import pytest

from datareservoirio import _constants
from datareservoirio.environments import Environment


class Test_Environment:
    @pytest.fixture
    def environment(self):
        environment = Environment()
        return environment

    def test__init__(self):
        environment = Environment()
        assert environment.current_environment == "PROD"
        assert environment.api_base_url == _constants.API_BASE_URL_PROD

    def test_api_base_url(self, environment):
        base_url_out = environment.api_base_url
        base_url_expect = _constants.API_BASE_URL_PROD
        assert base_url_out == base_url_expect

    def test_current_environment(self, environment):
        current_env_out = environment.current_environment
        current_env_expect = "PROD"
        assert current_env_out == current_env_expect

    def test_get(self, environment):
        get_out = environment.get()
        get_expect = "PROD"
        assert get_out == get_expect

    def test_set_default(self, environment):
        environment.set_default()
        assert environment.current_environment == "PROD"
        assert environment.api_base_url == _constants.API_BASE_URL_PROD
        assert (
            environment._application_insight_connectionstring
            == _constants.APPLICATIONINSIGHTS_PROD_CONNECTIONSTRING
        )

    def test_set_production(self, environment):
        environment.set_production()
        assert environment.current_environment == "PROD"
        assert environment.api_base_url == _constants.API_BASE_URL_PROD
        assert (
            environment._application_insight_connectionstring
            == _constants.APPLICATIONINSIGHTS_PROD_CONNECTIONSTRING
        )

    def test_set_qa(self, environment):
        environment.set_qa()
        assert environment.current_environment == "QA"
        assert environment.api_base_url == _constants.API_BASE_URL_QA
        assert (
            environment._application_insight_connectionstring
            == _constants.APPLICATIONINSIGHTS_QA_CONNECTIONSTRING
        )

    def test_set_test(self, environment):
        environment.set_test()
        assert environment.current_environment == "TEST"
        assert environment.api_base_url == _constants.API_BASE_URL_TEST
        assert (
            environment._application_insight_connectionstring
            == _constants.APPLICATIONINSIGHTS_TEST_CONNECTIONSTRING
        )

    def test_set_dev(self, environment):
        environment.set_dev()
        assert environment.current_environment == "DEV"
        assert environment.api_base_url == _constants.API_BASE_URL_DEV
        assert (
            environment._application_insight_connectionstring
            == _constants.APPLICATIONINSIGHTS_DEV_CONNECTIONSTRING
        )

    def test__set_environment(self, environment):
        environment._set_environment("QA")
        assert environment.current_environment == "QA"

    def test__set_base_url(self, environment):
        environment._set_base_url("https://foo/bar/baz")
        assert environment._api_base_url == "https://foo/bar/baz"

    def test__set_application_insight_connectionstring(self, environment):
        environment._set_application_insight_connectionstring("foo=1234;bar=baz")
        assert environment._application_insight_connectionstring == "foo=1234;bar=baz"
