import unittest
import warnings
from timeit import default_timer as timer
from unittest.mock import patch

from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

from datareservoirio.authenticate import Authenticator
from tests_integration._auth import USER


class Test_Authenticate(unittest.TestCase):

    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def test_authentication_succeeds_without_error(self, mock_input):
        start = timer()
        auth = Authenticator(USER.NAME, auth_force=True)
        auth.close()
        stop = timer()
        print("login took {} seconds".format(stop - start))

    @patch('getpass.getpass', return_value='the wrong password')
    def test_authentication_raises_error(self, mock_input):
        with self.assertRaises(InvalidGrantError):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                auth = Authenticator(USER.NAME, auth_force=True)
            auth.close()

    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def test_authentication_token(self, mock_input):
        auth = Authenticator(USER.NAME, auth_force=True)
        auth.close()
        self.assertIsInstance(auth.token, dict)


if __name__ == '__main__':
    unittest.main()
