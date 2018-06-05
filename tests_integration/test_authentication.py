import unittest
from timeit import default_timer as timer

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from adal.adal_error import AdalError

from datareservoirio.authenticate import Authenticator

from tests_integration._auth import USER


class Test_Authenticate(unittest.TestCase):

    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def test_authentication_succeeds_without_error(self, mock_input):
        start = timer()
        auth = Authenticator(USER.NAME)
        stop = timer()
        print("login took {} seconds".format(stop - start))

    @patch('getpass.getpass', return_value='the wrong password')
    def test_authentication_raises_error(self, mock_input):
        with self.assertRaises(AdalError):
            auth = Authenticator(USER.NAME)

    @patch('getpass.getpass', return_value=USER.PASSWORD)
    def test_authentication_token(self, mock_input):
        auth = Authenticator(USER.NAME)
        self.assertIsInstance(auth.token, dict)


if __name__ == '__main__':
    unittest.main()
