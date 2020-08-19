import unittest
import warnings
from timeit import default_timer as timer

from oauthlib.oauth2.rfc6749.errors import InvalidClientError

from datareservoirio.authenticate import ClientAuthenticator
from tests_integration._auth import CLIENT


class Test_Authenticate(unittest.TestCase):

    def test_authentication_succeeds_without_error(self):
        start = timer()
        auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        auth.close()
        stop = timer()
        print("login took {} seconds".format(stop - start))

    def test_authentication_raises_error(self):
        with self.assertRaises(InvalidClientError):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                auth = ClientAuthenticator(CLIENT.CLIENT_ID, "wrong secret")
            auth.close()

    def test_authentication_token(self):
        auth = ClientAuthenticator(CLIENT.CLIENT_ID, CLIENT.CLIENT_SECRET)
        auth.close()
        self.assertIsInstance(auth.token, dict)


if __name__ == '__main__':
    unittest.main()
