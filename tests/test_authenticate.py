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
            "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJlbWFpbCI6InZyc0A0c3Vic2VhLmNvbSIsImdpdmVuX25hbWUiOiJWZWdhcmQiLCJmYW1pbHlfbmFtZSI6IlLDuHJ2aWsgU29sdW0iLCJuYW1lIjoiVmVnYXJkIFLDuHJ2aWsgU29sdW0iLCJpZHAiOiI0c3Vic2VhLXByb2QtaXAiLCJvcmdhbml6YXRpb25JZCI6IjJjNGVlNTYyLTYyNjEtNDAxOC1hMWIxLTg4MzdhYjUyNjk0NCIsIm5hbWVpZGVudGlmaWVyIjoiNGY2NWM1N2MtMDY4Ny00YzNmLTllNzgtOWQ0NWQ1NTJmNmI1Iiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiZXh0ZXJuYWxJZCI6IjRzdWJzZWEtcHJvZC1pcHxkMmRlNzA0Mi04MDc0LTQwMzEtYjlhNC0wMGRkMzI0ZDA1NTMiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJzY3AiOiJ3cml0ZSByZWFkIiwiYXpwIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwidmVyIjoiMS4wIiwiaWF0IjoxNjgzMjg2NDk2LCJhdWQiOiJmZjQ3MzdiNS0zNjAyLTQ2YTAtOTgwNS1iZDE4MzE0NzAwYzEiLCJleHAiOjE2ODMyOTAwOTYsImlzcyI6Imh0dHBzOi8vNHN1YnNlYWlkLmIyY2xvZ2luLmNvbS9jOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAvdjIuMC8iLCJuYmYiOjE2ODMyODY0OTZ9.bz6c65fTe82Uig3DRjMahP_Zce7kBjzWVQiPyt-I3Pq7yGBrGNVVq0tJ87foscZdoaXZ9Ouyv-UV1UcF0_wgz_K7filb8FSz7RCai7LO8lEZ5XknLNCDlsQRzn0hqWYNHu-A-w1BRsisyyLDiTnYw1Xv4oPu6a6cLXGyRo8YoponhOEJ9Aa8WUNJe_fOTo4ssEwF46Tt66c-Pe2ZVfqFyqkUyksY4s7ub-bBm_-FWFJKjRWhxEy9tMCzt6PuTDjYdrfnhZZOYMiTwXsKByNotdf_eVgCHQf8s42fu5xmIXE7xFLWia1bHl7dHInayv0OCsmHL6H8msD4aClEfthgnA",
            "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJ2ZXIiOiIxLjAiLCJpc3MiOiJodHRwczovLzRzdWJzZWFpZC5iMmNsb2dpbi5jb20vYzhlYTExOGYtYmQ1MC00MjJlLTg1MDMtMWQ4MDU1YTNkY2YwL3YyLjAvIiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXVkIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwiZXhwIjoxNjgzMjkwMDk2LCJhY3IiOiJiMmNfMWFfc2lnbnVwb3JzaWduaW53aXRoNHNzX3Byb2QiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJpYXQiOjE2ODMyODY0OTYsImF1dGhfdGltZSI6MTY3OTMyNTQ1NSwiZW1haWwiOiJ2cnNANHN1YnNlYS5jb20iLCJnaXZlbl9uYW1lIjoiVmVnYXJkIiwiZmFtaWx5X25hbWUiOiJSw7hydmlrIFNvbHVtIiwibmFtZSI6IlZlZ2FyZCBSw7hydmlrIFNvbHVtIiwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIiwib3JnYW5pemF0aW9uSWQiOiIyYzRlZTU2Mi02MjYxLTQwMTgtYTFiMS04ODM3YWI1MjY5NDQiLCJuYW1laWRlbnRpZmllciI6IjRmNjVjNTdjLTA2ODctNGMzZi05ZTc4LTlkNDVkNTUyZjZiNSIsImV4dGVybmFsSWQiOiI0c3Vic2VhLXByb2QtaXB8ZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXRfaGFzaCI6InhzZV85d09qeUVLOEc0WERHNHJtVlEiLCJuYmYiOjE2ODMyODY0OTZ9.ZfLOHfkhyf_PwaXongoR8zI5LLBCMBOWDcyBMbgGs-nexcAXka6QKNwcefS9sCuTwHcwjlJd0iRFSm295GxQCW69dBQZYztjz-GGKlTK3jMC_6G5hV1GzWhZsjkjG0j5Jw-EsOAOEt21vQqpdKzwtrL7ODCwyoMaV1dD4BU6XUnaUUGdw-izEiKZXxQm1kjdHZyOt4zT99llcUcdNGOT7X_53iBJU5hpcxr3DHHSF7-IftAWJjj8pHBU6syFSfnecuu-_ktGtUwuX4EwcuR-OrSJHcULuoBoJwoU2p5VRxfTLvvPP6sVnpVC_Rdqkzicgr3GiS7654V4QGuT7PwwVg",
            "token_type": "Bearer",
            "not_before": 1683286496,
            "expires_in": 3600,
            "expires_on": 1683290096,
            "resource": "ff4737b5-3602-46a0-9805-bd18314700c1",
            "id_token_expires_in": 3600,
            "profile_info": "eyJ2ZXIiOiIxLjAiLCJ0aWQiOiJjOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAiLCJzdWIiOm51bGwsIm5hbWUiOiJWZWdhcmQgUsO4cnZpayBTb2x1bSIsInByZWZlcnJlZF91c2VybmFtZSI6bnVsbCwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIn0",
            "scope": [
                "https://4subseaid.onmicrosoft.com/reservoir-prod/write",
                "https://4subseaid.onmicrosoft.com/reservoir-prod/read",
                "openid",
                "offline_access",
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.QJhsCQLr66tuO7jXgFd_oHWFyGQqSp81EDkOxkghdcfb1VI8H8jmY6j8vkjSGHIx4pbJ3mxQgpDgI9Jy7LuXsqgmphdFUuUf6S1SyLovxvcTc47KADftkKGYTjKIs-O3g31zEvDn3MluIJENqGXyTyawteb_Lt8YQv3LE8kwHE9H4VszarcMNCbIW6QLtANraZyl2cRmdxOg2qawX97dsuBG3vVUgEXmh1hMQmVBJjH-Wx_0E8ueolxIOG4Is9ZqdAq57-ipXitbuRWYhBVxCGrlmjxLYAxAWvlA7DGTCyY_mmJOvQuRhSrAybny-lexVO6grEIce5-iuBQJ9im5fw.AsesX6FMO7ZAKY3o.9EnEMlBPxv96iz6-TICoJir1HbBY4nYHyp0nfzfsuuZCJpgA2khHPD5pNjitZ4p1BVvRKS8J8jVteXWJc5yhmDgnWjD4iQasLvpU6SIEb2a9jukYp2teh-maMwpG4d4gNwp0rZbcrzLDTH82yENIQZRYTJZYd3kDgcyMyXhH4L22keHasq796EHs6dAD8XVZtULfy47UwDdJ4L5kcK1uuKjAkUpDh1gkXwIggeo9GAVI3dYit_O5d4jovhWpp7rbu6V2iqw0A50OyEiU1o7uZPNl6V0P3Zir3yEBe9v_lEeYFQvyrEGp5x0OFhk_ifkiO7DyM0QiWCx2x_aK9yoUpN-8kyGTJ_7ZNKxqMJcV1s3S0BoBc1dSgWj15fB7cKSmB3wik38UJsmwrz8A-MAx0EIZleME3x7zne81Jk-ckO8BZnQ0sFszT9xN7iq3awWJVtMRoMC2JTYxaaRLt7Y8ZpVN4nfGUlIrzY8uihDpIIm1av33jvCvabWNYAZ6ojaaNp2V5VCPFDvJvWjB-XuNwTZXRaGqPA8-46xVnM0uYLLLM083-AWt3zDMZvUdXD1qle3E_o2FhrWX0QIqX7V02lrMUHvyRIPkMSkb1APZRjhQ2WQY514K6qKjnpoGVW1DhhR6ij99foUqbq8d1kFto9jm8-KZUklwj96sm4g9pw11MlVGXiNtpeOy-Y8jpCjc5f9sl7K0fmOFRMvztymkw_lmVd5MJ8t28sCHGKhBh2CwucJx7NY9qsP2Fatv3imPmcquW-H2Jv7GJfOcYGJjEKFrxII86LF93xXecBMSYtHx0CYrO2vtEckocd_GTxbDkOtZxTCimXprnKuMiQADCOjI6V-KfZCwTWVSMKvkTfScO_NZi1jgrqf9hutnd5Cxl1bD2Eg3jXApZa9YS3eLxMAavnPlVCCYnJAfTKTnx4omaTllMxDDgClxSaMBKjLZDzbj8xUftb9yXXfn0O5cEkE3BHC70U7eDkPhalAkLpcF0LMpCElGjq6paPeHTXXkH2jpNTzgiFu8UtwIiVJb6P0KUwn6leu7qARvOjDyRbvoRcdFHUdq-0a58_SpCuPAw8U.qUgvoULPonbSwzANOflqmg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1683290096.1777682,
        }

        assert not os.path.exists(token_root / "token.PROD")
        assert token_cache.token is None

        token_cache.dump(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache.token == token

    def test_dump_exists(self, token_cache2, token_root):
        token = {
            "token_url": "https://4subseaid.b2clogin.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token?p=B2C_1A_SignUpOrSignInWith4ss_prod",
            "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJlbWFpbCI6InZyc0A0c3Vic2VhLmNvbSIsImdpdmVuX25hbWUiOiJWZWdhcmQiLCJmYW1pbHlfbmFtZSI6IlLDuHJ2aWsgU29sdW0iLCJuYW1lIjoiVmVnYXJkIFLDuHJ2aWsgU29sdW0iLCJpZHAiOiI0c3Vic2VhLXByb2QtaXAiLCJvcmdhbml6YXRpb25JZCI6IjJjNGVlNTYyLTYyNjEtNDAxOC1hMWIxLTg4MzdhYjUyNjk0NCIsIm5hbWVpZGVudGlmaWVyIjoiNGY2NWM1N2MtMDY4Ny00YzNmLTllNzgtOWQ0NWQ1NTJmNmI1Iiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiZXh0ZXJuYWxJZCI6IjRzdWJzZWEtcHJvZC1pcHxkMmRlNzA0Mi04MDc0LTQwMzEtYjlhNC0wMGRkMzI0ZDA1NTMiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJzY3AiOiJ3cml0ZSByZWFkIiwiYXpwIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwidmVyIjoiMS4wIiwiaWF0IjoxNjgzMjg2NDk2LCJhdWQiOiJmZjQ3MzdiNS0zNjAyLTQ2YTAtOTgwNS1iZDE4MzE0NzAwYzEiLCJleHAiOjE2ODMyOTAwOTYsImlzcyI6Imh0dHBzOi8vNHN1YnNlYWlkLmIyY2xvZ2luLmNvbS9jOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAvdjIuMC8iLCJuYmYiOjE2ODMyODY0OTZ9.bz6c65fTe82Uig3DRjMahP_Zce7kBjzWVQiPyt-I3Pq7yGBrGNVVq0tJ87foscZdoaXZ9Ouyv-UV1UcF0_wgz_K7filb8FSz7RCai7LO8lEZ5XknLNCDlsQRzn0hqWYNHu-A-w1BRsisyyLDiTnYw1Xv4oPu6a6cLXGyRo8YoponhOEJ9Aa8WUNJe_fOTo4ssEwF46Tt66c-Pe2ZVfqFyqkUyksY4s7ub-bBm_-FWFJKjRWhxEy9tMCzt6PuTDjYdrfnhZZOYMiTwXsKByNotdf_eVgCHQf8s42fu5xmIXE7xFLWia1bHl7dHInayv0OCsmHL6H8msD4aClEfthgnA",
            "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJ2ZXIiOiIxLjAiLCJpc3MiOiJodHRwczovLzRzdWJzZWFpZC5iMmNsb2dpbi5jb20vYzhlYTExOGYtYmQ1MC00MjJlLTg1MDMtMWQ4MDU1YTNkY2YwL3YyLjAvIiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXVkIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwiZXhwIjoxNjgzMjkwMDk2LCJhY3IiOiJiMmNfMWFfc2lnbnVwb3JzaWduaW53aXRoNHNzX3Byb2QiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJpYXQiOjE2ODMyODY0OTYsImF1dGhfdGltZSI6MTY3OTMyNTQ1NSwiZW1haWwiOiJ2cnNANHN1YnNlYS5jb20iLCJnaXZlbl9uYW1lIjoiVmVnYXJkIiwiZmFtaWx5X25hbWUiOiJSw7hydmlrIFNvbHVtIiwibmFtZSI6IlZlZ2FyZCBSw7hydmlrIFNvbHVtIiwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIiwib3JnYW5pemF0aW9uSWQiOiIyYzRlZTU2Mi02MjYxLTQwMTgtYTFiMS04ODM3YWI1MjY5NDQiLCJuYW1laWRlbnRpZmllciI6IjRmNjVjNTdjLTA2ODctNGMzZi05ZTc4LTlkNDVkNTUyZjZiNSIsImV4dGVybmFsSWQiOiI0c3Vic2VhLXByb2QtaXB8ZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXRfaGFzaCI6InhzZV85d09qeUVLOEc0WERHNHJtVlEiLCJuYmYiOjE2ODMyODY0OTZ9.ZfLOHfkhyf_PwaXongoR8zI5LLBCMBOWDcyBMbgGs-nexcAXka6QKNwcefS9sCuTwHcwjlJd0iRFSm295GxQCW69dBQZYztjz-GGKlTK3jMC_6G5hV1GzWhZsjkjG0j5Jw-EsOAOEt21vQqpdKzwtrL7ODCwyoMaV1dD4BU6XUnaUUGdw-izEiKZXxQm1kjdHZyOt4zT99llcUcdNGOT7X_53iBJU5hpcxr3DHHSF7-IftAWJjj8pHBU6syFSfnecuu-_ktGtUwuX4EwcuR-OrSJHcULuoBoJwoU2p5VRxfTLvvPP6sVnpVC_Rdqkzicgr3GiS7654V4QGuT7PwwVg",
            "token_type": "Bearer",
            "not_before": 1683286496,
            "expires_in": 3600,
            "expires_on": 1683290096,
            "resource": "ff4737b5-3602-46a0-9805-bd18314700c1",
            "id_token_expires_in": 3600,
            "profile_info": "eyJ2ZXIiOiIxLjAiLCJ0aWQiOiJjOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAiLCJzdWIiOm51bGwsIm5hbWUiOiJWZWdhcmQgUsO4cnZpayBTb2x1bSIsInByZWZlcnJlZF91c2VybmFtZSI6bnVsbCwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIn0",
            "scope": [
                "https://4subseaid.onmicrosoft.com/reservoir-prod/write",
                "https://4subseaid.onmicrosoft.com/reservoir-prod/read",
                "openid",
                "offline_access",
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.QJhsCQLr66tuO7jXgFd_oHWFyGQqSp81EDkOxkghdcfb1VI8H8jmY6j8vkjSGHIx4pbJ3mxQgpDgI9Jy7LuXsqgmphdFUuUf6S1SyLovxvcTc47KADftkKGYTjKIs-O3g31zEvDn3MluIJENqGXyTyawteb_Lt8YQv3LE8kwHE9H4VszarcMNCbIW6QLtANraZyl2cRmdxOg2qawX97dsuBG3vVUgEXmh1hMQmVBJjH-Wx_0E8ueolxIOG4Is9ZqdAq57-ipXitbuRWYhBVxCGrlmjxLYAxAWvlA7DGTCyY_mmJOvQuRhSrAybny-lexVO6grEIce5-iuBQJ9im5fw.AsesX6FMO7ZAKY3o.9EnEMlBPxv96iz6-TICoJir1HbBY4nYHyp0nfzfsuuZCJpgA2khHPD5pNjitZ4p1BVvRKS8J8jVteXWJc5yhmDgnWjD4iQasLvpU6SIEb2a9jukYp2teh-maMwpG4d4gNwp0rZbcrzLDTH82yENIQZRYTJZYd3kDgcyMyXhH4L22keHasq796EHs6dAD8XVZtULfy47UwDdJ4L5kcK1uuKjAkUpDh1gkXwIggeo9GAVI3dYit_O5d4jovhWpp7rbu6V2iqw0A50OyEiU1o7uZPNl6V0P3Zir3yEBe9v_lEeYFQvyrEGp5x0OFhk_ifkiO7DyM0QiWCx2x_aK9yoUpN-8kyGTJ_7ZNKxqMJcV1s3S0BoBc1dSgWj15fB7cKSmB3wik38UJsmwrz8A-MAx0EIZleME3x7zne81Jk-ckO8BZnQ0sFszT9xN7iq3awWJVtMRoMC2JTYxaaRLt7Y8ZpVN4nfGUlIrzY8uihDpIIm1av33jvCvabWNYAZ6ojaaNp2V5VCPFDvJvWjB-XuNwTZXRaGqPA8-46xVnM0uYLLLM083-AWt3zDMZvUdXD1qle3E_o2FhrWX0QIqX7V02lrMUHvyRIPkMSkb1APZRjhQ2WQY514K6qKjnpoGVW1DhhR6ij99foUqbq8d1kFto9jm8-KZUklwj96sm4g9pw11MlVGXiNtpeOy-Y8jpCjc5f9sl7K0fmOFRMvztymkw_lmVd5MJ8t28sCHGKhBh2CwucJx7NY9qsP2Fatv3imPmcquW-H2Jv7GJfOcYGJjEKFrxII86LF93xXecBMSYtHx0CYrO2vtEckocd_GTxbDkOtZxTCimXprnKuMiQADCOjI6V-KfZCwTWVSMKvkTfScO_NZi1jgrqf9hutnd5Cxl1bD2Eg3jXApZa9YS3eLxMAavnPlVCCYnJAfTKTnx4omaTllMxDDgClxSaMBKjLZDzbj8xUftb9yXXfn0O5cEkE3BHC70U7eDkPhalAkLpcF0LMpCElGjq6paPeHTXXkH2jpNTzgiFu8UtwIiVJb6P0KUwn6leu7qARvOjDyRbvoRcdFHUdq-0a58_SpCuPAw8U.qUgvoULPonbSwzANOflqmg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1683290096.1777682,
        }

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache2.token is not None

        token_cache2.dump(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache2.token == token

    def test__call__(self, token_cache, token_root):
        token = {
            "token_url": "https://4subseaid.b2clogin.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token?p=B2C_1A_SignUpOrSignInWith4ss_prod",
            "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJlbWFpbCI6InZyc0A0c3Vic2VhLmNvbSIsImdpdmVuX25hbWUiOiJWZWdhcmQiLCJmYW1pbHlfbmFtZSI6IlLDuHJ2aWsgU29sdW0iLCJuYW1lIjoiVmVnYXJkIFLDuHJ2aWsgU29sdW0iLCJpZHAiOiI0c3Vic2VhLXByb2QtaXAiLCJvcmdhbml6YXRpb25JZCI6IjJjNGVlNTYyLTYyNjEtNDAxOC1hMWIxLTg4MzdhYjUyNjk0NCIsIm5hbWVpZGVudGlmaWVyIjoiNGY2NWM1N2MtMDY4Ny00YzNmLTllNzgtOWQ0NWQ1NTJmNmI1Iiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiZXh0ZXJuYWxJZCI6IjRzdWJzZWEtcHJvZC1pcHxkMmRlNzA0Mi04MDc0LTQwMzEtYjlhNC0wMGRkMzI0ZDA1NTMiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJzY3AiOiJ3cml0ZSByZWFkIiwiYXpwIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwidmVyIjoiMS4wIiwiaWF0IjoxNjgzMjg2NDk2LCJhdWQiOiJmZjQ3MzdiNS0zNjAyLTQ2YTAtOTgwNS1iZDE4MzE0NzAwYzEiLCJleHAiOjE2ODMyOTAwOTYsImlzcyI6Imh0dHBzOi8vNHN1YnNlYWlkLmIyY2xvZ2luLmNvbS9jOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAvdjIuMC8iLCJuYmYiOjE2ODMyODY0OTZ9.bz6c65fTe82Uig3DRjMahP_Zce7kBjzWVQiPyt-I3Pq7yGBrGNVVq0tJ87foscZdoaXZ9Ouyv-UV1UcF0_wgz_K7filb8FSz7RCai7LO8lEZ5XknLNCDlsQRzn0hqWYNHu-A-w1BRsisyyLDiTnYw1Xv4oPu6a6cLXGyRo8YoponhOEJ9Aa8WUNJe_fOTo4ssEwF46Tt66c-Pe2ZVfqFyqkUyksY4s7ub-bBm_-FWFJKjRWhxEy9tMCzt6PuTDjYdrfnhZZOYMiTwXsKByNotdf_eVgCHQf8s42fu5xmIXE7xFLWia1bHl7dHInayv0OCsmHL6H8msD4aClEfthgnA",
            "id_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVFU081OEZ2ZGQtWTA5NkN6VTZQQk9jVXJIa01KOHQ4THp5V2NDQl9WaDgiLCJ0eXAiOiJKV1QifQ.eyJ2ZXIiOiIxLjAiLCJpc3MiOiJodHRwczovLzRzdWJzZWFpZC5iMmNsb2dpbi5jb20vYzhlYTExOGYtYmQ1MC00MjJlLTg1MDMtMWQ4MDU1YTNkY2YwL3YyLjAvIiwic3ViIjoiZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXVkIjoiNmI4Nzk2MjItNGM1Mi00M2EzLWJhMjMtMmU5NTk1ZGQ5OTZiIiwiZXhwIjoxNjgzMjkwMDk2LCJhY3IiOiJiMmNfMWFfc2lnbnVwb3JzaWduaW53aXRoNHNzX3Byb2QiLCJub25jZSI6IjYzODE0OTIyMjU0MDQzODg4NS5OMkpsTkRBNU9HWXRNamswTnkwME9UWXhMV0V6TWpNdE5qRXpPV1l4TTJZNE5EZzNaRGRqWkdVek16TXRaalk1T1MwMFl6TTJMVGxsTUdFdE5HWTVOVEk1WVRaak9HWXciLCJpYXQiOjE2ODMyODY0OTYsImF1dGhfdGltZSI6MTY3OTMyNTQ1NSwiZW1haWwiOiJ2cnNANHN1YnNlYS5jb20iLCJnaXZlbl9uYW1lIjoiVmVnYXJkIiwiZmFtaWx5X25hbWUiOiJSw7hydmlrIFNvbHVtIiwibmFtZSI6IlZlZ2FyZCBSw7hydmlrIFNvbHVtIiwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIiwib3JnYW5pemF0aW9uSWQiOiIyYzRlZTU2Mi02MjYxLTQwMTgtYTFiMS04ODM3YWI1MjY5NDQiLCJuYW1laWRlbnRpZmllciI6IjRmNjVjNTdjLTA2ODctNGMzZi05ZTc4LTlkNDVkNTUyZjZiNSIsImV4dGVybmFsSWQiOiI0c3Vic2VhLXByb2QtaXB8ZDJkZTcwNDItODA3NC00MDMxLWI5YTQtMDBkZDMyNGQwNTUzIiwiYXRfaGFzaCI6InhzZV85d09qeUVLOEc0WERHNHJtVlEiLCJuYmYiOjE2ODMyODY0OTZ9.ZfLOHfkhyf_PwaXongoR8zI5LLBCMBOWDcyBMbgGs-nexcAXka6QKNwcefS9sCuTwHcwjlJd0iRFSm295GxQCW69dBQZYztjz-GGKlTK3jMC_6G5hV1GzWhZsjkjG0j5Jw-EsOAOEt21vQqpdKzwtrL7ODCwyoMaV1dD4BU6XUnaUUGdw-izEiKZXxQm1kjdHZyOt4zT99llcUcdNGOT7X_53iBJU5hpcxr3DHHSF7-IftAWJjj8pHBU6syFSfnecuu-_ktGtUwuX4EwcuR-OrSJHcULuoBoJwoU2p5VRxfTLvvPP6sVnpVC_Rdqkzicgr3GiS7654V4QGuT7PwwVg",
            "token_type": "Bearer",
            "not_before": 1683286496,
            "expires_in": 3600,
            "expires_on": 1683290096,
            "resource": "ff4737b5-3602-46a0-9805-bd18314700c1",
            "id_token_expires_in": 3600,
            "profile_info": "eyJ2ZXIiOiIxLjAiLCJ0aWQiOiJjOGVhMTE4Zi1iZDUwLTQyMmUtODUwMy0xZDgwNTVhM2RjZjAiLCJzdWIiOm51bGwsIm5hbWUiOiJWZWdhcmQgUsO4cnZpayBTb2x1bSIsInByZWZlcnJlZF91c2VybmFtZSI6bnVsbCwiaWRwIjoiNHN1YnNlYS1wcm9kLWlwIn0",
            "scope": [
                "https://4subseaid.onmicrosoft.com/reservoir-prod/write",
                "https://4subseaid.onmicrosoft.com/reservoir-prod/read",
                "openid",
                "offline_access",
            ],
            "refresh_token": "eyJraWQiOiIzdkl5Mm5YNndKUjR0UUEyX05xZi1xb2dWbXB0dkhJR016VU56M0J0SHRvIiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.QJhsCQLr66tuO7jXgFd_oHWFyGQqSp81EDkOxkghdcfb1VI8H8jmY6j8vkjSGHIx4pbJ3mxQgpDgI9Jy7LuXsqgmphdFUuUf6S1SyLovxvcTc47KADftkKGYTjKIs-O3g31zEvDn3MluIJENqGXyTyawteb_Lt8YQv3LE8kwHE9H4VszarcMNCbIW6QLtANraZyl2cRmdxOg2qawX97dsuBG3vVUgEXmh1hMQmVBJjH-Wx_0E8ueolxIOG4Is9ZqdAq57-ipXitbuRWYhBVxCGrlmjxLYAxAWvlA7DGTCyY_mmJOvQuRhSrAybny-lexVO6grEIce5-iuBQJ9im5fw.AsesX6FMO7ZAKY3o.9EnEMlBPxv96iz6-TICoJir1HbBY4nYHyp0nfzfsuuZCJpgA2khHPD5pNjitZ4p1BVvRKS8J8jVteXWJc5yhmDgnWjD4iQasLvpU6SIEb2a9jukYp2teh-maMwpG4d4gNwp0rZbcrzLDTH82yENIQZRYTJZYd3kDgcyMyXhH4L22keHasq796EHs6dAD8XVZtULfy47UwDdJ4L5kcK1uuKjAkUpDh1gkXwIggeo9GAVI3dYit_O5d4jovhWpp7rbu6V2iqw0A50OyEiU1o7uZPNl6V0P3Zir3yEBe9v_lEeYFQvyrEGp5x0OFhk_ifkiO7DyM0QiWCx2x_aK9yoUpN-8kyGTJ_7ZNKxqMJcV1s3S0BoBc1dSgWj15fB7cKSmB3wik38UJsmwrz8A-MAx0EIZleME3x7zne81Jk-ckO8BZnQ0sFszT9xN7iq3awWJVtMRoMC2JTYxaaRLt7Y8ZpVN4nfGUlIrzY8uihDpIIm1av33jvCvabWNYAZ6ojaaNp2V5VCPFDvJvWjB-XuNwTZXRaGqPA8-46xVnM0uYLLLM083-AWt3zDMZvUdXD1qle3E_o2FhrWX0QIqX7V02lrMUHvyRIPkMSkb1APZRjhQ2WQY514K6qKjnpoGVW1DhhR6ij99foUqbq8d1kFto9jm8-KZUklwj96sm4g9pw11MlVGXiNtpeOy-Y8jpCjc5f9sl7K0fmOFRMvztymkw_lmVd5MJ8t28sCHGKhBh2CwucJx7NY9qsP2Fatv3imPmcquW-H2Jv7GJfOcYGJjEKFrxII86LF93xXecBMSYtHx0CYrO2vtEckocd_GTxbDkOtZxTCimXprnKuMiQADCOjI6V-KfZCwTWVSMKvkTfScO_NZi1jgrqf9hutnd5Cxl1bD2Eg3jXApZa9YS3eLxMAavnPlVCCYnJAfTKTnx4omaTllMxDDgClxSaMBKjLZDzbj8xUftb9yXXfn0O5cEkE3BHC70U7eDkPhalAkLpcF0LMpCElGjq6paPeHTXXkH2jpNTzgiFu8UtwIiVJb6P0KUwn6leu7qARvOjDyRbvoRcdFHUdq-0a58_SpCuPAw8U.qUgvoULPonbSwzANOflqmg",
            "refresh_token_expires_in": 1209600,
            "expires_at": 1683290096.1777682,
        }

        assert not os.path.exists(token_root / "token.PROD")
        assert token_cache.token is None

        token_cache(token)

        assert os.path.exists(token_root / "token.PROD")
        assert token_cache.token == token

    def test_append(self, token_cache):
        token_cache.append("foo", "bar")
        assert token_cache.token["foo"] == "bar"