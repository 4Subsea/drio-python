import logging

from . import globalsettings  # wierd bug. must be called last?
from .authenticate import UserAuthenticator as Authenticator
from .client import Client

__version__ = "0.0.1"


def set_log_level(lvl):
    """
    Used to set the logging level to the specified value.

    Parameters
    ----------
    lvl : int
        We recommend to use logging.DEBUG, logging.WARNING etc. to set the
        desired logging level.
    """
    logging.getLogger(__name__).setLevel(lvl)


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

set_log_level(logging.WARNING)
