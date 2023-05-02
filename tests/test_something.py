class TestSomething:
    def test_something(self):
        pass

    def test_http_call(self, auth_session):
        response = auth_session.get("my/http/endpoint")
        response.raise_for_status()

    def test_http_call2(self, auth_session, response_cases):
        response_cases.set("general")
        response = auth_session.get("http://blob/dayfile/numeric")
        response.raise_for_status()

    # def test_http_call3(self, auth_session, response_case_handler):
    #     response_case_handler.update(
    #         {
    #             ("GET", "foo/bar/baz"): {
    #                 "status_code": 200,
    #                 "reason": "OK",
    #             },
    #         }
    #     )
    #     response = auth_session.get("foo/bar/baz")
    #     response.raise_for_status()
