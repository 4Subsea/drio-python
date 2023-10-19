import logging
import os
from functools import wraps

from opencensus.ext.azure.log_exporter import AzureLogHandler

import datareservoirio as drio

from ._constants import ENV_VAR_ENABLE_APP_INSIGHTS
from .globalsettings import environment

exceptions_logger = logging.getLogger(__name__ + "_exception_logger")
exceptions_logger.setLevel(logging.INFO)

if os.getenv(ENV_VAR_ENABLE_APP_INSIGHTS) is not None:
    env_var_lowered = os.environ[ENV_VAR_ENABLE_APP_INSIGHTS].lower()
    if env_var_lowered == "true" or env_var_lowered == "1":
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
