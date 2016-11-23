import unittest

#from .. import usercredentials

try:
    from unittest.mock import patch
except:
    from mock import patch

import sys

sys.path.append('../../')
import timeseriesclient
import timeseriesclient.usercredentials as usercredentials

class TestGetUserCredentials(unittest.TestCase):
    
    @patch('timeseriesclient.usercredentials.get_username')
    @patch('timeseriesclient.usercredentials.get_password')
    def test_username_returned_correctly(self, mock_pass, mock_user):
        expected_username = 'a@real.username'
        mock_user.return_value = expected_username
        mock_pass.return_value = 'not important'

        username, password = usercredentials.get_user_credentials()        

        mock_user.assert_called_with()
        self.assertEqual(username, expected_username)

    
    @patch('timeseriesclient.usercredentials.get_username')
    @patch('timeseriesclient.usercredentials.get_password')
    def test_password_returned_correctly(self, mock_pass, mock_user):
        expected_passwd = 'top secret'
        mock_user.return_value = 'not important'
        mock_pass.return_value = expected_passwd

        username, password = usercredentials.get_user_credentials()

        mock_pass.assert_called_with()
        self.assertEqual(password, expected_passwd)

class TestGetUsername(unittest.TestCase):

    @patch('timeseriesclient.usercredentials.input')
    def test_calls_six_moves_input(self, mock):
        expected_username = 'username entered by user'
        mock.return_value = expected_username

        username = usercredentials.get_username()

        mock.assert_called_with('Username: ')
        self.assertEqual(username, expected_username)

class TestGetPassword(unittest.TestCase):

    @patch('timeseriesclient.usercredentials.getpass.getpass')
    def test_calls_getpass(self, mock):
        expected_prompt = 'Password: '
        expected_password = 'a_top_secret_password'

        mock.return_value = expected_password

        password = usercredentials.get_password()

        mock.assert_called_with(expected_prompt)
        self.assertEqual(password, expected_password)
