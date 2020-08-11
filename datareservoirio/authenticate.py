import getpass
import json
import logging
import os
import warnings
from abc import ABCMeta, abstractmethod

from oauthlib.oauth2 import (
    BackendApplicationClient,
    InvalidGrantError,
    LegacyApplicationClient,
    WebApplicationClient,
)
from requests_oauthlib import OAuth2Session

from . import _constants
from .appdirs import user_data_dir
from .globalsettings import environment
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class TokenCache:
    def __init__(self, session_key=None):
        if not os.path.exists(self._token_root):
            os.makedirs(self._token_root)

        self._session_key = f".{session_key}" if session_key else ""
        self._token_url = None

    def __call__(self, token):
        self.dump(token)

    @property
    def _token_root(self):
        return user_data_dir("datareservoirio")

    @property
    def token_path(self):
        return os.path.join(
            self._token_root, f"token.{environment.get()}{self._session_key}"
        )

    @property
    def token_url(self):
        return self._token_url

    def dump(self, token):
        token["token_url"] = self.token_url
        with open(self.token_path, "w") as f:
            json.dump(token, f)

    @property
    def token(self):
        try:
            with open(self.token_path, "r") as f:
                token = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
        self._token_url = token.pop("token_url", None)
        return token


class BaseAuthSession(OAuth2Session, metaclass=ABCMeta):
    """
    Abstract class for authorized sessions.

    Parameters
    ----------
    client : ``oauthlib.oauth2`` client.
        A client passed on to ``requests_oauthlib.OAuth2Session``.
    auth_force : bool, optional
        Force re-authenticating the session (default is False)
    session_key : str, optional
        Unique identifier for an auth session. Can be used so that multiple
        instances can have independent auth/refresh cycles with the identity
        authority.
    kwargs : keyword arguments
        Keyword arguments passed on to ``requests_oauthlib.OAuth2Session``.
        Here, the mandatory parameters for the authentication client shall be
        provided.

    """

    def __init__(
        self, client, session_params, auth_force=False, session_key=None, **kwargs
    ):

        self._token_cache = TokenCache(session_key=session_key)
        self._token_url = session_params.pop("token_url", None)

        self._session_params = session_params

        super().__init__(
            client=client,
            token_updater=self._token_cache,
            token=self._token_cache.token,
            **kwargs,
        )

        self._token_url = self._token_url or self._token_cache.token_url

        if auth_force:
            token = self._fetch_token_initial()
        else:
            try:
                token = self.refresh_token()
                self.auto_refresh_url = self._token_url
                print("Authentication from previous session still valid.")
            except (KeyError, ValueError, InvalidGrantError):
                token = self._fetch_token_initial()
        self._token_cache(token)

    def _fetch_token_initial(self):
        """Define process for obtaining the initial token."""
        token = self.fetch_token()
        self.auto_refresh_url = self._token_url
        return token

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
        authority.

    """

    def __init__(self, auth_force=False, session_key=None):
        env = environment.current_environment

        client_id = eval(f"_constants.CLIENT_ID_{env}_USER")
        session_params = {
            "client_secret": eval(f"_constants.CLIENT_SECRET_{env}_USER"),
            "token_url": None,  # retrieved from access token response
            "authority": eval(f"_constants.AUTHORITY_URL_{env}_USER"),
        }

        client = WebApplicationClient(client_id)
        super().__init__(
            client, session_params, auth_force=auth_force, session_key=session_key
        )

    def _prepare_fetch_token_args(self):
        print("Please go here and authorize,", self._session_params["authority"])
        package = input("Paste code here: ")
        parameters = json.loads(package)
        token_url = parameters["endpoint"]
        code = parameters["code"]

        self._token_url = token_url
        self._token_cache._token_url = token_url

        args = (self._token_url,)
        kwargs = {"code": code, "client_secret": self._session_params["client_secret"]}
        return args, kwargs

    def _prepare_refresh_token_args(self):
        args = (self._token_url,)
        kwargs = {
            "refresh_token": self.token["refresh_token"],
            "client_secret": self._session_params["client_secret"],
        }
        return args, kwargs


class ClientAuthenticator(BaseAuthSession):
    """
    Authorized session where credentials are given as client_id and
    client_secret. When valid credentials are presented, the session is
    authenticated and persisted. A previous session will be reused as long as
    it is not expired. When required, a new authentication code is prompted
    for.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    client_id : str
        Unique identifier for the client (i.e. app/service etc.).
    client_secret : str
        Secret/password for the client.
    auth_force : bool, optional
        Force re-authenticating the session (default is False)
    session_key : str, optional
        Unique identifier for an auth session. Can be used so that multiple
        instances can have independent auth/refresh cycles with the identity
        authority.

    """

    def __init__(self, client_id, client_secret, session_key=None):
        env = environment.current_environment

        session_params = {
            "client_secret": client_secret,
            "token_url": eval(f"_constants.TOKEN_URL_{env}_CLIENT"),
            "scope": eval(f"_constants.SCOPE_{env}_CLIENT"),
        }

        client = BackendApplicationClient(client_id)
        super().__init__(
            client, session_params, auth_force=True, session_key=session_key
        )
        self.refresh_token = self.fetch_token

    def _prepare_fetch_token_args(self):
        args = (self._token_url,)
        kwargs = {
            "client_secret": self._session_params["client_secret"],
            "scope": self._session_params["scope"],
            "include_client_id": True,
        }
        return args, kwargs

    def _prepare_refresh_token_args(self):
        return


class AccessToken(UserAuthenticator):  # Deprecate soon
    def __init__(self, auth_force=False, session_key=None):
        warnings.warn("Use UserAuthenticator instead.", DeprecationWarning)
        super().__init__(self, auth_force=auth_force, session_key=session_key)


class UserCredentials(BaseAuthSession):  # Deprecate soon
    """
    Authorized session with username and password. Authenticates against legacy
    authority (Azure AD). Password is prompted when needed.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    username : str
        Username accepted by authority.
    auth_force : bool
        Force re-authenticating the session (default is False)

    """

    def __init__(self, username, auth_force=False):
        warnings.warn(
            "Support for username password credentials will be deprecated in "
            "the near future. Use 'ClienAuthenticator' instead."
        )

        env = environment.current_environment
        client_id = eval("_constants.CLIENT_ID_USERLEGACY")
        session_params = {
            "username": username,
            "resource": eval(f"_constants.RESOURCE_{env}_USERLEGACY"),
            "token_url": eval("_constants.TOKEN_URL_USERLEGACY"),
        }

        client = LegacyApplicationClient(client_id)
        super().__init__(client, session_params, auth_force=auth_force)

    def _prepare_fetch_token_args(self):
        args = (self._token_url,)
        kwargs = {
            "resource": self._session_params["resource"],
            "username": self._session_params["username"],
            "password": self._get_pass(),
            "include_client_id": True,
        }
        return args, kwargs

    def _prepare_refresh_token_args(self):
        args = (self._token_url,)
        kwargs = {"refresh_token": self.token["refresh_token"]}
        return args, kwargs

    def _get_pass(self):
        return getpass.getpass("Password: ")


class UnsafeUserCredentials(UserCredentials):  # Deprecate soon
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
        super().__init__(username, auth_force=True)

    def _get_pass(self):
        return self._password
