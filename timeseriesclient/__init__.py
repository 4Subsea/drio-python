import logging
from . import globalsettings

from .timeseriesclient import TimeSeriesClient

def set_log_level(lvl):
    logging.getLogger(__name__).setLevel(lvl)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

set_log_level(logging.WARNING)
