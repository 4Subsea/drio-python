import json
import os
import shutil
from pathlib import Path

import pytest

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
        assert os.path.exists(root_out)
        assert root_out == str(tmp_path / "datareservoirio")

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

    def test_dump(self, token_cache, token_root):
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
                "offline_access"
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.REDCk1-ZaxjZePMO9sp6X-6m0l0QaCyJCACh6tlNpiFHGFfheE3EXFXZ0d5N5oOaM6c9v5znrpCcMdr1Ztt1E2_ZW8L522FnKw1Z8Kh476u2q7XXs0Skd9_D3k0sbtM2375Rgtkbc3DjSF7j_jKgmGvupr97a_uW6t32Ezxagz_FQGpBybj7CrYIjzMco8MRRj2UbyPZB1tpZycYpqswbzHNMaEcbxWQ8lHLVSWLv_0VvQPtR0YU9qvf3tWjpQyUVWNfl-UYrSwFvbqm9wWDYhoKzQkYxX_YwK-r0A3UL_3HBfUxvTCy1fnPFCaIHAXDi5V5keLVfG0y1LuduiPKjA.AsLfz6271zVBbKxe.HKyWxqM2yDMPqSlKfw3hiJnJOjTiSqFy1Cmyrii1jY0GaBnZNklMxG3BwoDEXzEeSttpWk93h43dO-h3Bw7JJZ6DmaO_2nQa72BtOuQoCQtv-tEw7iZEbg5z1qgTyBg7xyxuaWHmei3BWTLtv1AYQ6Kdnt-kJQOBVVOX-lNvAmJvn7thhdw2aJ04HVv5BHpuIMbtirZoACjhMERUpmiG1Q2i3I56bnlMg3lIBRVnNMQPOPF84zwq3JlEAuT0R5hPNURe_v6IwymddbHY1IkyX3U6DOGw8we64dZA4Hl2EnMeCog_gLGpz8yay0ZCSVQV6OL1hfFRqKPRludAwekljU3DR86mewoEJTLtYOh74wLwvQGbD2K1LLGM_FBLrVyqYOUMrEz1jfWthpoeRveI528p2goC3hMcrMRHl_e-gwvV2wkIF0Lph8ti9O359EfLc0HkMMwwmcJwVWn4TKf6Ul2TpXpP4rlmtkRAqDBrl1ezR85GegQ10ugE-QlmLJ3vTNwzmA3ePCDWDfwjgw4wuS6S-pJ0DKGiaQ5N8cN85YUhINwmbT8sLyd_D8P9U9ik6PsS6d2hHamrlP91sIkq1kn_ofPgqBnffNqhVUn_WXA9T2eqeYvpwKGZLFyJjokk41860zonqEpCAkwu-_U-61WJ5A5MYVSM3DnF1RGTJrkkz4TENrsjYUVYYKtkEtk7boL1EnCm3W5mnV7bTNj4qKgHeEM3CNOzc1phZjgIriYRBfxPdnLhxKQVLnutgO0qn2vUZkzf1aYdC9al8v4CLnhQ4kCXwnfnn28Tc2RkBPiLXcDeApoS_wwoF7K9CaCVX4S6WVT-PzbgAdGeX4muI_ZvmCzP_t3cnsPCibOwD7O4z0W81NVUy8egLtb-CeKPWey54G7eZyoFCZFkddT8W-61fkIZE9UTWaE1ix80u6irHH8ZzomWv6XDvdL_VdSL2RqY3DqyjkFmmqXiVBQvIuGVv_xjecwy5lCqW6VLSxvCzN3VH2RPhUybFsHuCFDCjzqm4J5YS1tVgwHX2gnCf-cgVdtK4TaNX_pNFQRHOjkN4GUOenR2jVbRS2KXbWgWcfsHv6nk.QnVmWp3X0H2q6pAriIIzHg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1682676602.6278732
        }

        assert not os.path.exists(token_root / "token.PROD")
        assert token_cache.token is None

        token_cache.dump(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache.token == token

    def test_dump_exists(self, token_cache2, token_root):
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
                "offline_access"
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.REDCk1-ZaxjZePMO9sp6X-6m0l0QaCyJCACh6tlNpiFHGFfheE3EXFXZ0d5N5oOaM6c9v5znrpCcMdr1Ztt1E2_ZW8L522FnKw1Z8Kh476u2q7XXs0Skd9_D3k0sbtM2375Rgtkbc3DjSF7j_jKgmGvupr97a_uW6t32Ezxagz_FQGpBybj7CrYIjzMco8MRRj2UbyPZB1tpZycYpqswbzHNMaEcbxWQ8lHLVSWLv_0VvQPtR0YU9qvf3tWjpQyUVWNfl-UYrSwFvbqm9wWDYhoKzQkYxX_YwK-r0A3UL_3HBfUxvTCy1fnPFCaIHAXDi5V5keLVfG0y1LuduiPKjA.AsLfz6271zVBbKxe.HKyWxqM2yDMPqSlKfw3hiJnJOjTiSqFy1Cmyrii1jY0GaBnZNklMxG3BwoDEXzEeSttpWk93h43dO-h3Bw7JJZ6DmaO_2nQa72BtOuQoCQtv-tEw7iZEbg5z1qgTyBg7xyxuaWHmei3BWTLtv1AYQ6Kdnt-kJQOBVVOX-lNvAmJvn7thhdw2aJ04HVv5BHpuIMbtirZoACjhMERUpmiG1Q2i3I56bnlMg3lIBRVnNMQPOPF84zwq3JlEAuT0R5hPNURe_v6IwymddbHY1IkyX3U6DOGw8we64dZA4Hl2EnMeCog_gLGpz8yay0ZCSVQV6OL1hfFRqKPRludAwekljU3DR86mewoEJTLtYOh74wLwvQGbD2K1LLGM_FBLrVyqYOUMrEz1jfWthpoeRveI528p2goC3hMcrMRHl_e-gwvV2wkIF0Lph8ti9O359EfLc0HkMMwwmcJwVWn4TKf6Ul2TpXpP4rlmtkRAqDBrl1ezR85GegQ10ugE-QlmLJ3vTNwzmA3ePCDWDfwjgw4wuS6S-pJ0DKGiaQ5N8cN85YUhINwmbT8sLyd_D8P9U9ik6PsS6d2hHamrlP91sIkq1kn_ofPgqBnffNqhVUn_WXA9T2eqeYvpwKGZLFyJjokk41860zonqEpCAkwu-_U-61WJ5A5MYVSM3DnF1RGTJrkkz4TENrsjYUVYYKtkEtk7boL1EnCm3W5mnV7bTNj4qKgHeEM3CNOzc1phZjgIriYRBfxPdnLhxKQVLnutgO0qn2vUZkzf1aYdC9al8v4CLnhQ4kCXwnfnn28Tc2RkBPiLXcDeApoS_wwoF7K9CaCVX4S6WVT-PzbgAdGeX4muI_ZvmCzP_t3cnsPCibOwD7O4z0W81NVUy8egLtb-CeKPWey54G7eZyoFCZFkddT8W-61fkIZE9UTWaE1ix80u6irHH8ZzomWv6XDvdL_VdSL2RqY3DqyjkFmmqXiVBQvIuGVv_xjecwy5lCqW6VLSxvCzN3VH2RPhUybFsHuCFDCjzqm4J5YS1tVgwHX2gnCf-cgVdtK4TaNX_pNFQRHOjkN4GUOenR2jVbRS2KXbWgWcfsHv6nk.QnVmWp3X0H2q6pAriIIzHg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1682676602.6278732
        }

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache2.token is not None

        token_cache2.dump(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache2.token == token

    def test__call__(self, token_cache, token_root):
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
                "offline_access"
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.REDCk1-ZaxjZePMO9sp6X-6m0l0QaCyJCACh6tlNpiFHGFfheE3EXFXZ0d5N5oOaM6c9v5znrpCcMdr1Ztt1E2_ZW8L522FnKw1Z8Kh476u2q7XXs0Skd9_D3k0sbtM2375Rgtkbc3DjSF7j_jKgmGvupr97a_uW6t32Ezxagz_FQGpBybj7CrYIjzMco8MRRj2UbyPZB1tpZycYpqswbzHNMaEcbxWQ8lHLVSWLv_0VvQPtR0YU9qvf3tWjpQyUVWNfl-UYrSwFvbqm9wWDYhoKzQkYxX_YwK-r0A3UL_3HBfUxvTCy1fnPFCaIHAXDi5V5keLVfG0y1LuduiPKjA.AsLfz6271zVBbKxe.HKyWxqM2yDMPqSlKfw3hiJnJOjTiSqFy1Cmyrii1jY0GaBnZNklMxG3BwoDEXzEeSttpWk93h43dO-h3Bw7JJZ6DmaO_2nQa72BtOuQoCQtv-tEw7iZEbg5z1qgTyBg7xyxuaWHmei3BWTLtv1AYQ6Kdnt-kJQOBVVOX-lNvAmJvn7thhdw2aJ04HVv5BHpuIMbtirZoACjhMERUpmiG1Q2i3I56bnlMg3lIBRVnNMQPOPF84zwq3JlEAuT0R5hPNURe_v6IwymddbHY1IkyX3U6DOGw8we64dZA4Hl2EnMeCog_gLGpz8yay0ZCSVQV6OL1hfFRqKPRludAwekljU3DR86mewoEJTLtYOh74wLwvQGbD2K1LLGM_FBLrVyqYOUMrEz1jfWthpoeRveI528p2goC3hMcrMRHl_e-gwvV2wkIF0Lph8ti9O359EfLc0HkMMwwmcJwVWn4TKf6Ul2TpXpP4rlmtkRAqDBrl1ezR85GegQ10ugE-QlmLJ3vTNwzmA3ePCDWDfwjgw4wuS6S-pJ0DKGiaQ5N8cN85YUhINwmbT8sLyd_D8P9U9ik6PsS6d2hHamrlP91sIkq1kn_ofPgqBnffNqhVUn_WXA9T2eqeYvpwKGZLFyJjokk41860zonqEpCAkwu-_U-61WJ5A5MYVSM3DnF1RGTJrkkz4TENrsjYUVYYKtkEtk7boL1EnCm3W5mnV7bTNj4qKgHeEM3CNOzc1phZjgIriYRBfxPdnLhxKQVLnutgO0qn2vUZkzf1aYdC9al8v4CLnhQ4kCXwnfnn28Tc2RkBPiLXcDeApoS_wwoF7K9CaCVX4S6WVT-PzbgAdGeX4muI_ZvmCzP_t3cnsPCibOwD7O4z0W81NVUy8egLtb-CeKPWey54G7eZyoFCZFkddT8W-61fkIZE9UTWaE1ix80u6irHH8ZzomWv6XDvdL_VdSL2RqY3DqyjkFmmqXiVBQvIuGVv_xjecwy5lCqW6VLSxvCzN3VH2RPhUybFsHuCFDCjzqm4J5YS1tVgwHX2gnCf-cgVdtK4TaNX_pNFQRHOjkN4GUOenR2jVbRS2KXbWgWcfsHv6nk.QnVmWp3X0H2q6pAriIIzHg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1682676602.6278732
        }

        assert not os.path.exists(token_root / "token.PROD")
        assert token_cache.token is None

        token_cache(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache.token == token

    def test_append(self, token_cache):
        token_cache.append("foo", "bar")
        assert token_cache.token["foo"] == "bar"
