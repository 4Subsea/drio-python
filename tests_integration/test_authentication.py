import unittest
from timeit import default_timer as timer

from mock import patch
from adal.adal_error import AdalError

from datareservoirio.authenticate import Authenticator


USERNAME = 'reservoir-integrationtest@4subsea.com'
PASSWORD = 'qz9uVgNhANncz9Jp'


class Test_Authenticate(unittest.TestCase):

    @patch('getpass.getpass', return_value=PASSWORD)
    def test_authentication_succeeds_without_error(self, mock_input):
        start = timer()
        auth = Authenticator(USERNAME)
        stop = timer()
        print("login took {} seconds".format(stop - start))

    @patch('getpass.getpass', return_value='the wrong password')
    def test_authentication_raises_error(self, mock_input):
        with self.assertRaises(AdalError):
            auth = Authenticator(USERNAME)

    @patch('getpass.getpass', return_value=PASSWORD)
    def test_authentication_token(self, mock_input):
        auth = Authenticator(USERNAME)
        self.assertIsInstance(auth.token, dict)


if __name__ == '__main__':
    unittest.main()
