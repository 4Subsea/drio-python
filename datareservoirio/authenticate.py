from __future__ import absolute_import, division, print_function

import getpass
import json
import logging
import os
import uuid
import warnings
from abc import ABCMeta, abstractmethod
from base64 import urlsafe_b64encode

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from oauthlib.oauth2 import (
    LegacyApplicationClient,
    WebApplicationClient,
    InvalidGrantError
)
from requests_oauthlib import OAuth2Session

from .appdirs import user_data_dir
from . import _constants
from .globalsettings import environment
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)

# Python 2 compatibility
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class OAuth2Parameters(object):
    def __init__(self, environment, legacy_auth=True):
        self._environment = environment

        if legacy_auth:
            self._set_legacy()
        else:
            self._set_B2C()

    @property
    def authority(self):
        return self._authority

    @property
    def client_id(self):
        return self._client_id

    @property
    def resource(self):
        return self._resource

    @property
    def token_url(self):
        return self._token_url

    @property
    def client_secret(self):
        return self._client_secret

    @property
    def scope(self):
        return self._scope

    @property
    def redirect_uri(self):
        return self._redirect_uri

    def _set_legacy(self):
        self._authority = _constants.AUTHORITY_URL_LEGACY
        self._client_id = _constants.CLIENT_ID_LEGACY
        self._token_url = _constants.TOKEN_URL_LEGACY
        self._resource = getattr(
            _constants, 'RESOURCE_{}_LEGACY'.format(self._environment))

    def _set_B2C(self):
        self._authority = getattr(
            _constants, 'AUTHORITY_URL_{}'.format(self._environment))
        self._client_id = getattr(
            _constants, 'CLIENT_ID_{}'.format(self._environment))
        self._client_secret = getattr(
            _constants, 'CLIENT_SECRET_{}'.format(self._environment))
        self._redirect_uri = getattr(
            _constants, 'REDIRECT_URI_{}'.format(self._environment))
        self._token_url = getattr(
            _constants, 'TOKEN_URL_{}'.format(self._environment))
        self._scope = getattr(
            _constants, 'SCOPE_{}'.format(self._environment))


class TokenCache(object):

    def __init__(self):
        if not os.path.exists(self._token_root):
            os.makedirs(self._token_root)
        self._scrambler_init()

    def __call__(self, token):
        self.dump(token)

    @property
    def _token_root(self):
        return user_data_dir('datareservoirio')

    @property
    def token_path(self):
        return os.path.join(self._token_root, 'token')

    def dump(self, token):
        data = json.dumps(token).encode('utf-8')
        with open(self.token_path, 'wb') as f:
            f.write(self._scrambler.encrypt(data))

    def load(self):
        try:
            with open(self.token_path, 'rb') as f:
                data = self._scrambler.decrypt(f.read())
        except (FileNotFoundError, InvalidToken):
            return None
        else:
            return json.loads(data.decode('utf-8'))

    def _scrambler_init(self):
        machine_env = '{}|{}'.format(hex(uuid.getnode()), environment.get())

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=machine_env.encode('utf-8'),
            iterations=100000,
            backend=default_backend())
        key = urlsafe_b64encode(kdf.derive(self._token_root.encode('utf-8')))
        self._scrambler = Fernet(key)


class BaseAuthSession(OAuth2Session):
    """
    Abstract class for authorized sessions.

    Parameters
    ----------
    client : ``oauthlib.oauth2`` client.
        A client passed on to ``requests_oauthlib.OAuth2Session``.
    username : str
        Username accepted by authority.
    kwargs : keyword arguments
        Keyword arguments passed on to ``requests_oauthlib.OAuth2Session``.

    """
    __meta__ = ABCMeta

    def __init__(self, client, auth_force=False, **kwargs):
        self._params = getattr(self, '_params', self._get_params())

        token_cache = TokenCache()
        super(BaseAuthSession, self).__init__(
            client=client, auto_refresh_url=self._params.token_url,
            token_updater=token_cache, token=token_cache.load(), **kwargs)

        if auth_force:
            token = self._fetch_token_initial()
        else:
            try:
                token = self.refresh_token()
                print('Authentication from previous session still valid.')
            except (KeyError, InvalidGrantError):
                token = self._fetch_token_initial()
        token_cache(token)

    @abstractmethod
    def _fetch_token_initial(self):
        """Define process for obtaining the initial token."""
        pass

    def fetch_token(self):
        """Fetch new access and refresh token."""
        args, kwargs = self._prepare_fetch_token_args()
        token = super(BaseAuthSession, self).fetch_token(*args, **kwargs)
        return token

    def refresh_token(self, *args, **kwargs):
        """Refresh (expired) access token with a valid refresh token."""
        args, kwargs = self._prepare_refresh_token_args()
        token = super(BaseAuthSession, self).refresh_token(*args, **kwargs)
        return token

    @abstractmethod
    def _prepare_fetch_token_args(self):
        """
        Prepare positional and keyword arguments passed on to
        ``OAuth2Session.fetch_token``. Subclass overrides.
        """
        args = ()
        kwargs = {}
        return args, kwargs

    @abstractmethod
    def _prepare_refresh_token_args(self):
        """
        Prepare positional and keyword arguments passed on to
        ``OAuth2Session.refresh_token``. Subclass overrides.
        """
        args = ()
        kwargs = {}
        return args, kwargs

    def _get_params(self, **kwargs):
        return OAuth2Parameters(environment.current_environment, **kwargs)


class UserCredentials(BaseAuthSession):
    """
    Authorized session with username and password. Authenticates against legacy
    authority (Azure AD). Password is prompted when needed.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    username : str
        Username accepted by authority.

    """

    def __init__(self, username, auth_force=False):
        self.username = username
        self._params = self._get_params(legacy_auth=True)
        client = LegacyApplicationClient(self._params.client_id)

        super(UserCredentials, self).__init__(client, auth_force=auth_force)

    def _fetch_token_initial(self):
        return self.fetch_token()

    def _prepare_fetch_token_args(self):
        args = (self._params.token_url, )
        kwargs = {
            'client_id': self._params.client_id,
            'resource': self._params.resource,
            'username': self.username,
            'password': self._get_pass()
        }
        return args, kwargs

    def _prepare_refresh_token_args(self):
        args = (self._params.token_url, )
        kwargs = {'refresh_token': self.token['refresh_token']}
        return args, kwargs

    def _get_pass(self):
        return getpass.getpass('Password: ')


class UnsafeUserCredentials(UserCredentials):
    """
    Authorized session with username and password. Authenticates against legacy
    authority (Azure AD). It is considered "unsafe" as it stores password in
    memory.

    Extends ``UserCredentials``.

    Parameters
    ----------
    username : str
        Username accepted by authority.
    password : str
        Password accepted by authority.

    """

    def __init__(self, username, password):

        self._password = password
        super(UnsafeUserCredentials, self).__init__(username, auth_force=True)

    def _get_pass(self):
        return self._password


class AccessTokenLoginB2C(BaseAuthSession):
    """
    Authorized session where credentials are given in browser. Authenticates
    against authority using the B2C protocol. Password is prompted when needed.

    Extends ``BaseAuthSession``.

    """
    def __init__(self, auth_force=False):
        self._params = self._get_params(legacy_auth=False)
        client = WebApplicationClient(self._params.client_id)
        super(AccessTokenLoginB2C, self).__init__(client, auth_force=auth_force)

    def _fetch_token_initial(self):
        return self.fetch_token()

    def _prepare_fetch_token_args(self):
        print('Please go here and authorize,', self._params.authority)
        code = input('Paste code here: ')
        args = (self._params.token_url, )
        kwargs = {
            'code': code,
            'client_secret': self._params.client_secret
        }
        return args, kwargs

    def _prepare_refresh_token_args(self):
        args = (self._params.token_url, )
        kwargs = {
            'refresh_token': self.token['refresh_token'],
            'client_secret': self._params.client_secret
            }
        return args, kwargs


# Swap these when default authenticator shall use B2C
Authenticator = UserCredentials
# Authenticator = AccessTokenLoginB2C


# Legacy support and backward compatibility
class AdalAuthenticator(UserCredentials):
    def __init__(self, username):
        warnings.warn('AdalAuthenticator will be depracated in near future.'
                      ' Please switch to UserCredentials.', DeprecationWarning)
        super(AdalAuthenticator, self).__init__(username, auth_force=True)


class UnsafeAdalAuthenticator(UnsafeUserCredentials):
    def __init__(self, username, password):
        warnings.warn('UnsafeAdalAuthenticator will be depracated in near future.'
                      ' Please switch to UnsafeUserCredentials.', DeprecationWarning)
        super(UnsafeAdalAuthenticator, self).__init__(username, password)
