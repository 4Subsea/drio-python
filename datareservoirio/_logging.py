import logging
import os
from functools import cache, wraps

from azure.monitor.opentelemetry import configure_azure_monitor

import datareservoirio as drio

from ._constants import ENV_VAR_ENABLE_APP_INSIGHTS, ENV_VAR_ENGINE_ROOM_APP_ID
from .globalsettings import environment


@cache
def get_exceptions_logger() -> logging.Logger:
    exceptions_logger = logging.getLogger(__name__ + "_exception_logger")
    exceptions_logger.setLevel(logging.DEBUG)

    if os.getenv(ENV_VAR_ENABLE_APP_INSIGHTS) is not None:
        enable_app_insights = os.environ[ENV_VAR_ENABLE_APP_INSIGHTS].lower()
        if enable_app_insights == "true" or enable_app_insights == "1":
            configure_azure_monitor(
                connection_string=environment._application_insight_connectionstring,
                logger_name=__name__ + "_exceptions_logger",
            )
            exceptions_logger.setLevel("WARNING")

    return exceptions_logger


def log_decorator(log_level):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                properties = {
                    "customDimensions": {
                        "drioPackage": f"python-datareservoirio/{drio.__version__}",
                    }
                }
                if os.getenv(ENV_VAR_ENGINE_ROOM_APP_ID) is not None:
                    properties["customDimensions"]["engineRoomAppId"] = os.getenv(
                        ENV_VAR_ENGINE_ROOM_APP_ID
                    )

                log_function = getattr(get_exceptions_logger(), log_level)
                log_function(e, extra=properties)
                raise e

        return wrapper

    return decorator
