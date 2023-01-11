import json
import logging
import os
from abc import ABCMeta, abstractmethod

from oauthlib.oauth2 import (
    BackendApplicationClient,
    InvalidGrantError,
    WebApplicationClient,
)
from requests_oauthlib import OAuth2Session

import datareservoirio as drio

from . import _constants  # noqa: F401
from .appdirs import user_data_dir
from .globalsettings import environment

log = logging.getLogger(__name__)


class TokenCache:
    def __init__(self, session_key=None):
        self._session_key = f".{session_key}" if session_key else ""

        if not os.path.exists(self._token_root):
            os.makedirs(self._token_root)

        try:
            with open(self.token_path, "r") as f:
                self._token = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._token = {}

    def __call__(self, token):
        self.dump(token)

    def append(self, key, value):
        self._token[key] = value

    @property
    def _token_root(self):
        return user_data_dir("datareservoirio")

    @property
    def token_path(self):
        return os.path.join(
            self._token_root, f"token.{environment.get()}{self._session_key}"
        )

    def dump(self, token):
        self._token.update(token)
        with open(self.token_path, "w") as f:
            json.dump(self._token, f)

    @property
    def token(self):
        return self._token or None


class BaseAuthSession(OAuth2Session, metaclass=ABCMeta):
    """
    Abstract class for authorized sessions.

    Parameters
    ----------
    client : ``oauthlib.oauth2`` client.
        A client passed on to ``requests_oauthlib.OAuth2Session``.
    token_url : str
        Token endpoint URL, must use HTTPS.
    session_params: dict
        Dictionary containing the necessary parameters used during
        authentication. Use these parameters when overriding the
        '_prepare_fetch_token_args' and '_prepare_refresh_token_args' methods.
        The provided dict is stored internally in '_session_params'.
    auth_force : bool, optional
        Force re-authenticating the session (default is False)
    kwargs : keyword arguments
        Keyword arguments passed on to ``requests_oauthlib.OAuth2Session``.
        Here, the mandatory parameters for the authentication client shall be
        provided.

    """

    def __init__(self, client, auth_force=False, **kwargs):
        super().__init__(
            client=client,
            **kwargs,
        )

        if auth_force or not self.token:
            token = self.fetch_token()
        else:
            try:
                token = self.refresh_token()
            except (KeyError, ValueError, InvalidGrantError):
                token = self.fetch_token()

        if self.token_updater:
            self.token_updater(token)

        self.headers.update(
            {"user-agent": f"python-datareservoirio/{drio.__version__}"}
        )

    def fetch_token(self):
        """Fetch new access and refresh token."""
        args, kwargs = self._prepare_fetch_token_args()
        token = super().fetch_token(*args, **kwargs)
        return token

    def refresh_token(self, *args, **kwargs):
        """Refresh (expired) access token with a valid refresh token."""
        args, kwargs = self._prepare_refresh_token_args()
        token = super().refresh_token(*args, **kwargs)
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


class UserAuthenticator(BaseAuthSession):
    """
    Authorized session where credentials are given in the DataReservoir.io web
    application. When a valid code is presented, the session is authenticated
    and persisted. A previous session will be reused as long as it is not
    expired. When required, a new authentication code is prompted for.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    auth_force : bool, optional
        Force re-authenticating the session (default is False)
    session_key : str, optional
        Unique identifier for an auth session. Can be used so that multiple
        instances can have independent auth/refresh cycles with the identity
        authority. Prevents local cache from being accidently overwritten.

    """

    def __init__(self, auth_force=False, session_key=None):
        self._env = environment.current_environment

        token_cache = TokenCache(session_key=session_key)
        token = token_cache.token

        if token:
            self._token_url = token.get("token_url", None)
        else:
            self._token_url = None

        client = WebApplicationClient(eval(f"_constants.CLIENT_ID_{self._env}_USER"))
        super().__init__(
            client,
            auth_force=auth_force,
            token_updater=token_cache,
            token=token,
            auto_refresh_url=self._token_url,
        )

    def _prepare_fetch_token_args(self):
        print(
            "Please go here and authorize,",
            eval(f"_constants.AUTHORITY_URL_{self._env}_USER"),
        )
        package = input("Paste code here: ")
        parameters = json.loads(package)
        token_url = parameters["endpoint"]
        code = parameters["code"]

        self.token_updater.append("token_url", token_url)
        self._token_url = token_url
        self.auto_refresh_url = token_url

        args = (self._token_url,)
        kwargs = {
            "code": code,
            "client_secret": eval(f"_constants.CLIENT_SECRET_{self._env}_USER"),
        }
        return args, kwargs

    def _prepare_refresh_token_args(self):
        args = (self._token_url,)
        kwargs = {
            "refresh_token": self.token["refresh_token"],
            "client_secret": eval(f"_constants.CLIENT_SECRET_{self._env}_USER"),
        }
        return args, kwargs


class ClientAuthenticator(BaseAuthSession):
    """
    Authorized session where credentials are given as client_id and
    client_secret. When valid credentials are presented, the session is
    authenticated and persisted.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    client_id : str
        Unique identifier for the client (i.e. app/service etc.).
    client_secret : str
        Secret/password for the client.

    """

    def __init__(self, client_id, client_secret):
        self._env = environment.current_environment
        self._client_secret = client_secret

        client = BackendApplicationClient(client_id)
        super().__init__(
            client,
            auth_force=True,
            auto_refresh_url=eval(f"_constants.TOKEN_URL_{self._env}_CLIENT"),
            # unable to supress TokenUpdated expection without this dummy updater
            token_updater=lambda token: None,
        )

    def _prepare_fetch_token_args(self):
        args = (eval(f"_constants.TOKEN_URL_{self._env}_CLIENT"),)
        kwargs = {
            "client_secret": self._client_secret,
            "scope": eval(f"_constants.SCOPE_{self._env}_CLIENT"),
            "include_client_id": True,
        }
        return args, kwargs

    def _prepare_refresh_token_args(self):
        return

    def refresh_token(self, *args, **kwargs):
        """Refresh (expired) access token"""
        token = self.fetch_token()
        return token
