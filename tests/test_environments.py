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

    def test_get(self, environment):
        get_out = environment.get()
        get_expect = "PROD"
        assert get_out == get_expect

    def test_set_qa(self, environment):
        environment.set_qa()
        assert environment.current_environment == "QA"
        assert environment.api_base_url == "https://reservoir-api-qa.4subsea.net/api/"
        assert environment._application_insight_connectionstring == "InstrumentationKey=aec779fc-7a4c-4580-b205-cd9f8ecdcf48;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/;LiveEndpoint=https://westeurope.livediagnostics.monitor.azure.com/"

    def test_set_test(self, environment):
        environment.set_test()
        assert environment.current_environment == "TEST"
        assert environment.api_base_url == "https://reservoir-api-test.4subsea.net/api/"
        assert environment._application_insight_connectionstring == "InstrumentationKey=725af0e5-7530-4f2c-b055-36258831785e;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/;LiveEndpoint=https://westeurope.livediagnostics.monitor.azure.com/"
