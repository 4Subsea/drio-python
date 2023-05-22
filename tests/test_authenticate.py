import json
import os
import shutil
from pathlib import Path
from unittest.mock import Mock

import pytest
from requests_oauthlib import OAuth2Session

import datareservoirio as drio
from datareservoirio.authenticate import (
    BaseAuthSession,
    ClientAuthenticator,
    TokenCache,
    UserAuthenticator,
)

TEST_PATH = Path(__file__).parent


class Test_TokenCache:
    @pytest.fixture
    def token(self):
        token = {
            "token_url": "https://4subseaid.b2clogin.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token?p=B2C_1A_SignUpOrSignInWith4ss_prod",
            "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJlbWFpbCI6InZyc0A0c3Vic2VhLmNvbSIsImdpdmVuX25hbWUiOiJWZWdhcmQiLCJmYW1pbHlfbmFtZSI6IlLDuHJ2aWsgU29sdW0iLCJuYW1lIjoiVmVnYXJkIFLDuHJ2aWsgU29sdW0iLCJpZHAiOiI0c3Vic2VhLXByb2QtaXAiLCJvcmdhbml6YXRpb25JZCI6IjJjNGVlNTYyLTYyNjEtNDAxOC1hMWIxLTg4MzdhYjUyNjk0NCIsIm5hbWVpZGVudGlmaWVyIjoiNGY2NWM1N2MtMDY4Ny00YzNmLTllNzgtOWQ0NWQ1NTJmNmI1Iiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiZXh0ZXJuYWxJZCI6IjRzdWJzZWEtcHJvZC1pcHxkMmRlNzA0Mi04MDc0LTQwMzEtYjlhNC0wMGRkMzI0ZDA1NTMiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJzY3AiOiJ3cml0ZSByZWFkIiwiYXpwIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwidmVyIjoiMS4wIiwiaWF0IjoxNjgyNjczMDAyLCJhdWQiOiJmZjQ3MzdiNS0zNjAyLTQ2YTAtOTgwNS1iZDE4MzE0NzAwYzEiLCJleHAiOjE2ODI2NzY2MDIsImlzcyI6Imh0dHBzOi8vNHN1YnNlYWlkLmIyY2xvZ2luLmNvbS9jOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAvdjIuMC8iLCJuYmYiOjE2ODI2NzMwMDJ9.owWGOv8mQDWMNXPX-EH1sk_Qik53_2Y0PBHmknEKPZuOuPk-HFC_XFwaiAIX_zA_-rFUBKDt3MA2ro3Vx6N3wXUPv8papWKup7O5m7yaSTnTI1iPHqAXRdK5Se0dYaunuSLiS4ZYwrI06gxcwWC9JgT5Yx4L41jsFhzPC6Zoe73RG3PbIbDmAXAIl5fhNnswoIhCb6NB_QCZciKmgntLsIEUmuytkUA2OFh3gUYaynAaKxZhzvzL6KMsYjSRO7HAg45YSvsM3UgiwzomSc_b_c8ARIJ1tIsoEhiDl1qvlX5GQZhCjcss_pEdjazfGCNi3xXCBFhzTik0V_0xeLnWAg",
            "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJ2ZXIiOiIxLjAiLCJpc3MiOiJodHRwczovLzRzdWJzZWFpZC5iMmNsb2dpbi5jb20vYzhlYTExOGYtYmQ1MC00MjJlLTg1MDMtMWQ4MDU1YTNkY2YwL3YyLjAvIiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXVkIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwiZXhwIjoxNjgyNjc2NjAyLCJhY3IiOiJiMmNfMWFfc2lnbnVwb3JzaWduaW53aXRoNHNzX3Byb2QiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJpYXQiOjE2ODI2NzMwMDIsImF1dGhfdGltZSI6MTY3OTMyNTQ1NSwiZW1haWwiOiJ2cnNANHN1YnNlYS5jb20iLCJnaXZlbl9uYW1lIjoiVmVnYXJkIiwiZmFtaWx5X25hbWUiOiJSw7hydmlrIFNvbHVtIiwibmFtZSI6IlZlZ2FyZCBSw7hydmlrIFNvbHVtIiwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIiwib3JnYW5pemF0aW9uSWQiOiIyYzRlZTU2Mi02MjYxLTQwMTgtYTFiMS04ODM3YWI1MjY5NDQiLCJuYW1laWRlbnRpZmllciI6IjRmNjVjNTdjLTA2ODctNGMzZi05ZTc4LTlkNDVkNTUyZjZiNSIsImV4dGVybmFsSWQiOiI0c3Vic2VhLXByb2QtaXB8ZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXRfaGFzaCI6ImsyaHNNdmlIT2RYNl9zRVFndXVyb3ciLCJuYmYiOjE2ODI2NzMwMDJ9.a3AF5QrmDmJcCHZa7Bm8WANSHdw_0DixZHIgc3wF70465NSI0rYhTz8ZWA809HMwRJQnnSSn4PPesMOGCXt5Vpqgdqeks2o0aiYoSnQS3t5D1_0HUltAWM_qLvxQ_i0O1dGtGVQ-txY3LeqIq9H3Ql6KZcQjEFh50HotVYhmLRqUwymxSlQNyfGx-8zc99k7r0Obe0ZmOZlOOdipNNT4DCdiU5pG2B6rVtzlG1V27ZthWvkFclhn12zySUsyL8Lu8c7cGCE83-8M7jnPL-6hE4-_gSsVcXvTq-ph7tc42bfHuo4ixXOOHbx08C_tRakFXHHPj005QB28zEX2whZYwg",
            "token_type": "Bearer",
            "not_before": 1682673002,
            "expires_in": 3600,
            "expires_on": 1682676602,
            "resource": "ff4737b5-3602-46a0-9805-bd18314700c1",
            "id_token_expires_in": 3600,
            "profile_info": "eyJ2ZXIiOiIxLjAiLCJ0aWQiOiJjOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAiLCJzdWIiOm51bGwsIm5hbWUiOiJWZWdhcmQgUsO4cnZpayBTb2x1bSIsInByZWZlcnJlZF91c2VybmFtZSI6bnVsbCwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIn0",
            "scope": [
                "https://4subseaid.onmicrosoft.com/reservoir-prod/write",
                "https://4subseaid.onmicrosoft.com/reservoir-prod/read",
                "openid",
                "offline_access",
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.REDCk1-ZaxjZePMO9sp6X-6m0l0QaCyJCACh6tlNpiFHGFfheE3EXFXZ0d5N5oOaM6c9v5znrpCcMdr1Ztt1E2_ZW8L522FnKw1Z8Kh476u2q7XXs0Skd9_D3k0sbtM2375Rgtkbc3DjSF7j_jKgmGvupr97a_uW6t32Ezxagz_FQGpBybj7CrYIjzMco8MRRj2UbyPZB1tpZycYpqswbzHNMaEcbxWQ8lHLVSWLv_0VvQPtR0YU9qvf3tWjpQyUVWNfl-UYrSwFvbqm9wWDYhoKzQkYxX_YwK-r0A3UL_3HBfUxvTCy1fnPFCaIHAXDi5V5keLVfG0y1LuduiPKjA.AsLfz6271zVBbKxe.HKyWxqM2yDMPqSlKfw3hiJnJOjTiSqFy1Cmyrii1jY0GaBnZNklMxG3BwoDEXzEeSttpWk93h43dO-h3Bw7JJZ6DmaO_2nQa72BtOuQoCQtv-tEw7iZEbg5z1qgTyBg7xyxuaWHmei3BWTLtv1AYQ6Kdnt-kJQOBVVOX-lNvAmJvn7thhdw2aJ04HVv5BHpuIMbtirZoACjhMERUpmiG1Q2i3I56bnlMg3lIBRVnNMQPOPF84zwq3JlEAuT0R5hPNURe_v6IwymddbHY1IkyX3U6DOGw8we64dZA4Hl2EnMeCog_gLGpz8yay0ZCSVQV6OL1hfFRqKPRludAwekljU3DR86mewoEJTLtYOh74wLwvQGbD2K1LLGM_FBLrVyqYOUMrEz1jfWthpoeRveI528p2goC3hMcrMRHl_e-gwvV2wkIF0Lph8ti9O359EfLc0HkMMwwmcJwVWn4TKf6Ul2TpXpP4rlmtkRAqDBrl1ezR85GegQ10ugE-QlmLJ3vTNwzmA3ePCDWDfwjgw4wuS6S-pJ0DKGiaQ5N8cN85YUhINwmbT8sLyd_D8P9U9ik6PsS6d2hHamrlP91sIkq1kn_ofPgqBnffNqhVUn_WXA9T2eqeYvpwKGZLFyJjokk41860zonqEpCAkwu-_U-61WJ5A5MYVSM3DnF1RGTJrkkz4TENrsjYUVYYKtkEtk7boL1EnCm3W5mnV7bTNj4qKgHeEM3CNOzc1phZjgIriYRBfxPdnLhxKQVLnutgO0qn2vUZkzf1aYdC9al8v4CLnhQ4kCXwnfnn28Tc2RkBPiLXcDeApoS_wwoF7K9CaCVX4S6WVT-PzbgAdGeX4muI_ZvmCzP_t3cnsPCibOwD7O4z0W81NVUy8egLtb-CeKPWey54G7eZyoFCZFkddT8W-61fkIZE9UTWaE1ix80u6irHH8ZzomWv6XDvdL_VdSL2RqY3DqyjkFmmqXiVBQvIuGVv_xjecwy5lCqW6VLSxvCzN3VH2RPhUybFsHuCFDCjzqm4J5YS1tVgwHX2gnCf-cgVdtK4TaNX_pNFQRHOjkN4GUOenR2jVbRS2KXbWgWcfsHv6nk.QnVmWp3X0H2q6pAriIIzHg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1682676602.6278732,
        }
        return token

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

        monkeypatch.setattr(
            "datareservoirio.authenticate.user_data_dir", mock_user_data_dir
        )

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
        root_expect = str(tmp_path / "datareservoirio")
        assert os.path.exists(root_out)
        assert root_out == root_expect

    def test_token_path(self, token_cache, token_root):
        path_out = token_cache.token_path
        path_expect = os.path.join(token_root, "token.PROD")
        assert path_out == path_expect

    def test_token_path_with_session_key(self, token_root):
        token_cache = TokenCache(session_key="foobar")
        path_out = token_cache.token_path
        path_expect = os.path.join(token_root, "token.PROD.foobar")
        assert path_out == path_expect

    def test_token_none(self, token_cache):
        token_out = token_cache.token
        assert token_out is None

    def test_token_exists(self, token_cache2):
        token_out = token_cache2.token
        with open(TEST_PATH / "testdata" / "tokens" / "token.PROD", "r") as f:
            token_expect = json.load(f)
        assert token_out == token_expect

    def test_token_exists_session_key(self, copytokens):
        token_cache = TokenCache(session_key="foobar")
        token_out = token_cache.token
        with open(TEST_PATH / "testdata" / "tokens" / "token.PROD.foobar", "r") as f:
            token_expect = json.load(f)
        assert token_out == token_expect

    def test_dump(self, token_cache, token_root, token):
        assert not os.path.exists(token_root / "token.PROD")
        assert token_cache.token is None

        token_cache.dump(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache.token == token

    def test_dump_exists(self, token_cache2, token_root, token):
        assert os.path.exists(token_root / "token.PROD")
        assert token_cache2.token is not None

        token_cache2.dump(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache2.token == token

    def test__call__(self, token_cache, token_root, token):
        assert not os.path.exists(token_root / "token.PROD")
        assert token_cache.token is None

        token_cache(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache.token == token

    def test_append(self, token_cache):
        token_cache.append("foo", "bar")
        assert token_cache.token["foo"] == "bar"


class Test_UserAuthenticator:
    @pytest.fixture(autouse=True)
    def set_response_cases(self, response_cases):
        response_cases.set("datareservoirio-oauth2")

    @pytest.fixture
    def token_prod(self):
        with open(
            TEST_PATH
            / "testdata"
            / "response_cases"
            / "datareservoirio_oauth2"
            / "token.PROD.json",
            mode="r",
        ) as f:
            token = json.load(f)
        return token

    @pytest.fixture
    def endpoint_code(self):
        package = {
            "endpoint": "https://4subseaid.b2clogin.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token?p=B2C_1A_SignUpOrSignInWith4ss_prod",
            "code": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.Pa0qRh7A4Ajb-YmKs144LI02tLCZSf256Wse5ZQg1yWp52GlWrUa7rLf17Xeby9CxSqbey3CX-erJDTAoewdSOFE8svsiPMTBOuom697i4OFKpmkRpp_4j650LHwuGi7DvGEsbIZ6h5XF6ggz870QEuY5BFt5eXZ0AEm_v15vxiEBbqkQnLmc6F1jh8Ln7omCicj2uWkhay015zLTVwBp6LsRakWwqFw0TtIkr2Ip49SyxzaFfoZxa5s9QcoDa0Ytmkw6OR7ndgJHCS0nbx3_z3QY248XlX9jMQl38q4VNR0wZgDAXK2LCP6YD4TXSCao1SUICLfKdLvVYsODia_1w.8ywp9Q0fcdRh_OxM.qlWd2uQZaOl2Mnjcj9sEbZLqoTknvC6acbTIYEGv-O8n9QSr5m7amQUxVaUg5CYkQ0_lupU0rJqgeP-grUa_a9zia5PC6r4MnT1z7zK-raXFMnhBDRY33FZdWR6FdhiO173nnwvjpxchrxrZG-XnX2VMmCkVgOcebmUvIcYA79tIVqupUin_R_ly0cLze6UhOcU-cPfZ_wKMy5RJHO0f9CtuQC-Mq6Q5O7QElmo7vK8UYCouwyD0_ku-2qZawFQLGpCwMo9dVy5rlmu5CIkP8H_0BEcY6zLWpUGbcBEwajxV6p6XsdiRYsqUJC3Nf20o1LdMBtsxjq-Zx52SKszFDFLx4RbJv_9BaDYOnZpNs2iogvhgSk8d3gzappNBW8-5V65J17ADvz7h3jpQtYlPrLKUHw_6jk0j0XvUTPmpHBADPD7i4twn2D6g-HZ9E9GF0AbEQR1EagGHkFPtA8DNYypxJbQYwB4dliIQ6-lbKcjpVGw_WHyhK8Wagl22kre9Z_SHuuOzloILp4guaK4ifqgjVBLVMBo7hqMwN_6fB7H0wiQ5TfTcola92DYpJw4h_K039GuvpDJR6mV6-__Iq3_dbcYWzBLGfOiyl-fMT0BlmxKtOMbQ0yq0srKhjuQqp2F8spF7hW2vjqrGoIMfAMphu40QD3pNOBOHGmd-Ly4Wxh23DnNv97S6eoFhxmPqdj_GM7Pe0lXTYE_4lqjuA_PLdlnDEIeKJiD7H2IIcC4V0EdhbkbRhSoXGXkBfB0Pg2ECY4JFndzTfksvklv9lWBwaFdEPD05swSYwKENDxppsaOwzf0jiejqig-71z33ypMF2ltjRzRZ23N5rcvGgQ6fQaoF96BVPEzoWC92M78-6QAlVIBvc0OHem9GW51HQWrmVuus5h1gXbuS8UW2xZ4ui3KqfsBuBkOza1pS2eWiXiNWSETjNQUWL3ThSO6DutfKR1TkUY37TYPsHB-fUb_FNDm1My7TrPq1i4ZTHMZjk7WEh3JLa2WWJf_OQM_nGNXZ-DIh4C2DHieEiwapyxW6esVB6QZ3yWcRbBVBkHJBM4vVpokyUxM.ygUmVmPx63NwF5f_tAukTQ",
        }
        return package

    @pytest.fixture(autouse=True)
    def mock_input(self, monkeypatch, endpoint_code):
        endpoint_code = json.dumps(endpoint_code)
        mock_input = Mock(wraps=lambda *args, **kwargs: endpoint_code)
        monkeypatch.setattr("builtins.input", mock_input)
        return mock_input

    @pytest.fixture(autouse=True)
    def mock_user_data_dir(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "datareservoirio.authenticate.user_data_dir",
            lambda appname, *args, **kwargs: str(tmp_path / appname),
        )

    @pytest.fixture
    def add_tokens_to_token_root(self, tmp_path):
        # copy tokens to the token root
        token_root = tmp_path / "datareservoirio"
        token_root.mkdir(exist_ok=True)
        src = TEST_PATH / "testdata" / "tokens"
        for file_i in src.iterdir():
            shutil.copyfile(file_i, token_root / file_i.name)

    @pytest.fixture
    def user_authenticator(self):
        auth = UserAuthenticator(auth_force=False, session_key=None)
        return auth

    def test__init__(self, mock_input, token_prod, tmp_path, mock_requests):
        auth = UserAuthenticator(auth_force=False, session_key=None)

        assert isinstance(auth, OAuth2Session)
        assert isinstance(auth, BaseAuthSession)
        assert auth.client_id == drio._constants.CLIENT_ID_PROD_USER
        assert auth.access_token == token_prod["access_token"]
        assert (
            auth.headers["user-agent"] == f"python-datareservoirio/{drio.__version__}"
        )
        mock_input.assert_called_once()
        assert os.path.exists(tmp_path / "datareservoirio" / "token.PROD")
        assert (
            mock_requests.call_args.kwargs["data"]["grant_type"] == "authorization_code"
        )

    def test__init__token_available(self, add_tokens_to_token_root, mock_requests):
        UserAuthenticator(auth_force=False, session_key=None)
        assert mock_requests.call_args.kwargs["data"]["grant_type"] == "refresh_token"

    def test__init__session_key(self, tmp_path):
        assert not os.path.exists(tmp_path / "datareservoirio" / "token.PROD.foobar")
        UserAuthenticator(auth_force=False, session_key="foobar")
        assert os.path.exists(tmp_path / "datareservoirio" / "token.PROD.foobar")

    def test__init__auth_force(
        self, add_tokens_to_token_root, mock_requests, mock_input
    ):
        UserAuthenticator(auth_force=True)
        mock_input.assert_called_once()
        assert (
            mock_requests.call_args.kwargs["data"]["grant_type"] == "authorization_code"
        )

    def test__prepare_fetch_token_args(
        self, user_authenticator, endpoint_code, mock_input
    ):
        args_out, kwargs_out = user_authenticator._prepare_fetch_token_args()

        args_expect = (endpoint_code["endpoint"],)
        kwargs_expect = {
            "code": endpoint_code["code"],
            "client_secret": drio._constants.CLIENT_SECRET_PROD_USER,
        }
        assert args_out == args_expect
        assert kwargs_out == kwargs_expect
        assert (
            user_authenticator.token_updater.token["token_url"]
            == endpoint_code["endpoint"]
        )
        mock_input.assert_called()

    def test__prepare_refresh_token_args(
        self, user_authenticator, endpoint_code, token_prod
    ):
        args_out, kwargs_out = user_authenticator._prepare_refresh_token_args()

        args_expect = (endpoint_code["endpoint"],)
        kwargs_expect = {
            "refresh_token": token_prod["refresh_token"],
            "client_secret": drio._constants.CLIENT_SECRET_PROD_USER,
        }
        assert args_out == args_expect
        assert kwargs_out == kwargs_expect

    def test_fetch_token(
        self, user_authenticator, token_prod, mock_requests, endpoint_code
    ):
        token_out = user_authenticator.fetch_token()
        token_expect = token_prod
        token_out.pop("expires_at")
        token_expect.pop("expires_at")
        assert token_out == token_prod
        assert (
            mock_requests.call_args.kwargs["data"]["grant_type"] == "authorization_code"
        )
        assert mock_requests.call_args.kwargs["data"]["code"] == endpoint_code["code"]

    def test_refresh_token(self, user_authenticator, token_prod, mock_requests):
        token_out = user_authenticator.refresh_token()
        token_expect = token_prod
        token_out.pop("expires_at")
        token_expect.pop("expires_at")
        assert token_out == token_prod
        assert mock_requests.call_args.kwargs["data"]["grant_type"] == "refresh_token"
        assert (
            mock_requests.call_args.kwargs["data"]["client_secret"]
            == drio._constants.CLIENT_SECRET_PROD_USER
        )
        assert (
            mock_requests.call_args.kwargs["data"]["refresh_token"]
            == token_prod["refresh_token"]
        )


class Test_ClientAuthenticator:
    @pytest.fixture(autouse=True)
    def set_response_cases(self, response_cases):
        response_cases.set("datareservoirio-oauth2")

    @pytest.fixture
    def client_authenticator(self):
        auth = ClientAuthenticator("foo", "bar")
        return auth

    @pytest.fixture
    def token(self):
        with open(
            TEST_PATH
            / "testdata"
            / "response_cases"
            / "datareservoirio_oauth2"
            / "client_credentials_token.json",
            mode="r",
        ) as f:
            token = json.load(f)
        return token

    def test__init__(self, mock_requests):
        auth = ClientAuthenticator("foo", "bar")

        assert isinstance(auth, OAuth2Session)
        assert isinstance(auth, BaseAuthSession)
        assert auth.client_id == "foo"
        assert (
            auth.access_token
            == "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyJ9.eyJhdWQiOiJmZjQ3MzdiNS0zNjAyLTQ2YTAtOTgwNS1iZDE4MzE0NzAwYzEiLCJpc3MiOiJodHRwczovL2xvZ2luLm1pY3Jvc29mdG9ubGluZS5jb20vYzhlYTExOGYtYmQ1MC00MjJlLTg1MDMtMWQ4MDU1YTNkY2YwL3YyLjAiLCJpYXQiOjE2ODQ3NDY3OTUsIm5iZiI6MTY4NDc0Njc5NSwiZXhwIjoxNjg0NzUwNjk1LCJhaW8iOiJBU1FBMi84VEFBQUFSRHhMWHBZMEJFWkE3L2dhdW5xVnhQM0t3WEd5UnVqLzh2cm51bG04VUtjPSIsImF6cCI6IjBlZTVkMWI0LTIyNzEtNDU5NS04YzIzLWYyMzYxZDcxM2E0NyIsImF6cGFjciI6IjEiLCJvaWQiOiJmMzRjM2IwOC02ODMxLTQ4MzgtOTE5YS1lM2RkNmU4NGE4NTciLCJyaCI6IjAuQVNRQWp4SHF5RkM5TGtLRkF4MkFWYVBjOExVM1JfOENOcUJHbUFXOUdERkhBTUVrQUFBLiIsInJvbGVzIjpbIkFQSSJdLCJzdWIiOiJmMzRjM2IwOC02ODMxLTQ4MzgtOTE5YS1lM2RkNmU4NGE4NTciLCJ0aWQiOiJjOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAiLCJ1dGkiOiIyMURXT1p3bkZrZTRaVlJiNV9BbEFBIiwidmVyIjoiMi4wIn0.TP3SMojIrcwxfpXn2bAM9b3McdW-kJi9bhM9n46bEtiIVF4jb-EvUbJwiB80FdWCTdnr6DqY1xb1Njha8gVN5de0n_oY9AfqGRnGWuyBa1hMNBTdN59fGHb30aMYX4M4TqlfwirffKqCIyclLgm8JRzrHGbsYqXVF7v99ttRQY5hxhp8NH4G52NMXtd4VGMNjlyo8a6miWL_SZH0PD13DChESy4D1ROeD7pMtLv__R0tEJxZuxAg1LtskEg48bZc22WPnqo8OEnIGckPy7wwyUlfZBt_mSfqFv-DgZAUKCPzvUdUoW0Yk5KrWe6YvNdopUET9azIdZEgcMjy7Oe0og"
        )
        assert (
            auth.headers["user-agent"] == f"python-datareservoirio/{drio.__version__}"
        )

        assert mock_requests.call_args.kwargs["data"]["client_id"] == "foo"
        assert mock_requests.call_args.kwargs["data"]["client_secret"] == "bar"
        assert (
            mock_requests.call_args.kwargs["data"]["grant_type"] == "client_credentials"
        )

    def test__prepare_fetch_token_args(self, client_authenticator):
        args_out, kwargs_out = client_authenticator._prepare_fetch_token_args()

        args_expect = (drio._constants.TOKEN_URL_PROD_CLIENT,)
        kwargs_expect = {
            "client_secret": "bar",
            "scope": drio._constants.SCOPE_PROD_CLIENT,
            "include_client_id": True,
        }

        assert args_out == args_expect
        assert kwargs_out == kwargs_expect

    def test__prepare_refresh_token_args(self, client_authenticator):
        args_out = client_authenticator._prepare_refresh_token_args()
        assert args_out is None

    def test_fetch_token(self, client_authenticator, token, mock_requests):
        token_out = client_authenticator.fetch_token()
        token_expect = token
        token_out.pop("expires_at")
        token_expect.pop("expires_at")
        assert token_out == token_expect
        assert (
            mock_requests.call_args.kwargs["data"]["grant_type"] == "client_credentials"
        )

    def test_refresh_token(self, client_authenticator, token, mock_requests):
        token_out = client_authenticator.refresh_token()
        token_expect = token
        token_out.pop("expires_at")
        token_expect.pop("expires_at")
        assert token_out == token_expect
        assert (
            mock_requests.call_args.kwargs["data"]["grant_type"] == "client_credentials"
        )
