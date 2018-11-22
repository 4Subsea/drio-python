from __future__ import absolute_import, division, print_function

import logging
import pkg_resources

from .client import Client
from .authenticate import Authenticator
from . import globalsettings  # wierd bug. must be called last?


__version__ = pkg_resources.get_distribution('datareservoirio').version


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
