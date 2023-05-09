from datareservoirio.environments import Environment


class Test_Environment:
    def test__init__(self):
        env = Environment()
        assert env.current_environment == "PROD"
        assert env.api_base_url == "https://reservoir-api.4subsea.net/api/"
