import getpass
import json
import logging
import os
import platform
import warnings
from abc import ABCMeta, abstractmethod, abstractproperty
from base64 import urlsafe_b64encode

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from oauthlib.oauth2 import (
    BackendApplicationClient,
    LegacyApplicationClient,
    WebApplicationClient,
    InvalidGrantError,
)
from requests_oauthlib import OAuth2Session

from .appdirs import user_data_dir
from . import _constants
from .globalsettings import environment
from .log import LogWriter

logger = logging.getLogger(__name__)
logwriter = LogWriter(logger)


class OAuth2Parameters:
    def __init__(self, environment, auth_type):
        self._environment = environment
        self._auth_type = str(auth_type).upper()

        if self._auth_type == "USER":
            self._set_user()
        elif self._auth_type == "CLIENT":
            self._set_client()
        elif self._auth_type == "USERLEGACY":
            self._set_userlegacy()
        else:
            raise ValueError(
                "Invalid 'auth_type'. Must be 'USER', 'CLIENT', or 'USERCLIENT'."
            )

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

    @token_url.setter
    def token_url(self, value):
        self._token_url = value

    @property
    def client_secret(self):
        return self._client_secret

    @property
    def scope(self):
        return self._scope

    @property
    def redirect_uri(self):
        return self._redirect_uri

    def _set_userlegacy(self):
        self._authority = _constants.AUTHORITY_URL_USERLEGACY
        self._client_id = _constants.CLIENT_ID_USERLEGACY
        self._token_url = _constants.TOKEN_URL_USERLEGACY
        self._resource = getattr(
            _constants, "RESOURCE_{}_USERLEGACY".format(self._environment)
        )

    def _set_user(self):
        self._authority = getattr(
            _constants, "AUTHORITY_URL_{}_USER".format(self._environment)
        )
        self._client_id = getattr(
            _constants, "CLIENT_ID_{}_USER".format(self._environment)
        )
        self._client_secret = getattr(
            _constants, "CLIENT_SECRET_{}_USER".format(self._environment)
        )
        self._redirect_uri = getattr(
            _constants, "REDIRECT_URI_{}_USER".format(self._environment)
        )
        self._token_url = None  # token url will be provided as part of access code
        self._scope = getattr(_constants, "SCOPE_{}_USER".format(self._environment))

    def _set_client(self):
        self._client_id = None
        self._client_secret = None
        self._token_url = getattr(
            _constants, "TOKEN_URL_{}_CLIENT".format(self._environment)
        )
        self._scope = getattr(_constants, "SCOPE_{}_CLIENT".format(self._environment))


class TokenCache:
    def __init__(self, session_key=None):
        if not os.path.exists(self._token_root):
            os.makedirs(self._token_root)

        self._session_key = f".{session_key}" if session_key else ""
        self._token_url = None
        self._scrambler_init()

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
        data = json.dumps(token).encode("utf-8")
        with open(self.token_path, "wb") as f:
            f.write(self._scrambler.encrypt(data))

    def load(self):
        try:
            with open(self.token_path, "rb") as f:
                data = self._scrambler.decrypt(f.read())
        except (FileNotFoundError, InvalidToken):
            return None

        token = json.loads(data.decode("utf-8"))
        self._token_url = token.pop("token_url", None)
        return token

    def _scrambler_init(self):
        machine_env = "{}|{}".format(platform.node(), environment.get())

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=machine_env.encode("utf-8"),
            iterations=100000,
            backend=default_backend(),
        )
        key = urlsafe_b64encode(kdf.derive(self._token_root.encode("utf-8")))
        self._scrambler = Fernet(key)


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
        Unique identifier for an auth session. Can be used so that multiple instances can have
        independent auth/refresh cycles with the identity authority.
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

        super(BaseAuthSession, self).__init__(
            client=client,
            token_updater=self._token_cache,
            token=self._token_cache.load(),
            **kwargs,
        )

        if self._token_url is None:
            self._token_url = self._token_cache.token_url

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


class UserCredentials(BaseAuthSession):
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
        env = environment.current_environment
        client_id = eval("_constants.CLIENT_ID_USERLEGACY")
        session_params = {
            "username": username,
            "resource": eval(f"_constants.RESOURCE_{env}_USERLEGACY"),
            "token_url": eval("_constants.TOKEN_URL_USERLEGACY"),
        }

        client = LegacyApplicationClient(client_id)
        super(UserCredentials, self).__init__(
            client, session_params, auth_force=auth_force
        )

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


class AccessToken(BaseAuthSession):
    """
    Authorized session where credentials are given in the DataReservoir.io web application.
    When a valid code is presented, the session is authenticated and persisted. A previous session
    will be reused as long as it is not expired. When required, a new authentication code is prompted
    for.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    auth_force : bool, optional
        Force re-authenticating the session (default is False)
    session_key : str, optional
        Unique identifier for an auth session. Can be used so that multiple instances can have
        independent auth/refresh cycles with the identity authority.

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


class ServiceCredentials(BaseAuthSession):
    """
    Authorized session where credentials are given in the DataReservoir.io web application.
    When a valid code is presented, the session is authenticated and persisted. A previous session
    will be reused as long as it is not expired. When required, a new authentication code is prompted
    for.

    Extends ``BaseAuthSession``.

    Parameters
    ----------
    auth_force : bool, optional
        Force re-authenticating the session (default is False)
    session_key : str, optional
        Unique identifier for an auth session. Can be used so that multiple instances can have
        independent auth/refresh cycles with the identity authority.

    """
    def __init__(self, client_id, client_secret, session_key=None):
        env = environment.current_environment

        session_params = {
            "client_secret": client_secret,
            "token_url": eval(f"_constants.TOKEN_URL_{env}_CLIENT"),
            "scope": eval(f"_constants.SCOPE_{env}_CLIENT")
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


# Default authenticator
Authenticator = AccessToken


# Legacy support and backward compatibility
class AdalAuthenticator(UserCredentials):
    def __init__(self, username):
        warnings.warn(
            "AdalAuthenticator will be depracated in near future."
            " Please switch to UserCredentials.",
            DeprecationWarning,
        )
        super(AdalAuthenticator, self).__init__(username, auth_force=True)


class UnsafeAdalAuthenticator(UnsafeUserCredentials):
    def __init__(self, username, password):
        warnings.warn(
            "UnsafeAdalAuthenticator will be depracated in near future."
            " Please switch to UnsafeUserCredentials.",
            DeprecationWarning,
        )
        super(UnsafeAdalAuthenticator, self).__init__(username, password)
