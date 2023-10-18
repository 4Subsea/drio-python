import logging
import os
from functools import wraps

from opencensus.ext.azure.log_exporter import AzureLogHandler

import datareservoirio as drio

from .globalsettings import environment

exceptions_logger = logging.getLogger(__name__ + "_exception_logger")
exceptions_logger.setLevel(logging.INFO)

if os.getenv("APPLICATION_INSIGHTS_LOGGER") is not None:
    if os.environ["APPLICATION_INSIGHTS_LOGGER"] == "true":
        exceptions_logger.addHandler(
            AzureLogHandler(
                connection_string=environment._application_insight_connectionstring
            )
        )


def log_exception(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            properties = {
                "custom_dimensions": {
                    "drio_package": f"python-datareservoirio/{drio.__version__}"
                }
            }
            exceptions_logger.exception(e, extra=properties)
            raise e

    return wrapper
