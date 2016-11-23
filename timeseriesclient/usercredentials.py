import getpass

from six.moves import input

def get_user_credentials():
    return get_username(), get_password()
    

def get_username():
    return input('Username: ')

def get_password():
    return getpass.getpass('Password: ')
