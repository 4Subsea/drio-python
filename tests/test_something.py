import datareservoirio as drio


class TestSomething:

    def test_something(self):
        pass

    def test_http_call(self, auth_session, response_cases):
        response = auth_session.get("my/http/endpoint")
        response.raise_for_status()

    def test_http_call2(self, auth_session, response_cases):
        response_cases[("GET", "foo/bar/baz")] = {
            "status_code": 200,
            "reason": "OK",
        }
        response = auth_session.get("foo/bar/baz")
        response.raise_for_status()
