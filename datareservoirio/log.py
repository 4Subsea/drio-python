from __future__ import absolute_import, division, print_function

from datetime import datetime 


class LogWriter(object):

    def __init__(self, logger):
        self.logger = logger

    def critical(self, msg, origin=None):
        msg = self.format("CRITICAL", msg, origin)
        return self.logger.critical(msg)

    def error(self, msg, origin=None):
        msg = self.format("ERROR", msg, origin)
        return self.logger.error(msg)

    def warning(self, msg, origin=None):
        msg = self.format("WARNING", msg, origin)
        return self.logger.warning(msg)

    def info(self, msg, origin=None):
        msg = self.format("INFO", msg, origin)
        return self.logger.info(msg)

    def debug(self, msg, origin=None):
        msg = self.format("DEBUG", msg, origin)
        return self.logger.debug(msg)

    def format(self, lvl, msg, origin=None):
        time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        if origin:
            origin = '.'+origin
        else:
            origin = ''

        base = '{} *** {} *** {}{} *** {}'

        return base.format(time, lvl, self.logger.name, origin,  msg)


