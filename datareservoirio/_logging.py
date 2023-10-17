import logging
from functools import wraps
from .globalsettings import environment
from opencensus.ext.azure.log_exporter import AzureLogHandler

exceptions_logger = logging.getLogger(__name__ + "_exception_logger")
exceptions_logger.setLevel(logging.INFO)
exceptions_logger.addHandler(AzureLogHandler(connection_string=environment._application_insight_connectionstring))

def log_exception(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            exceptions_logger.error(e)
            raise e
    return wrapper