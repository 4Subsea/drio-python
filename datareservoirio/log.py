class LogWriter(object):
    def __init__(self, logger):
        self.logger = logger

    def critical(self, msg, origin=None):
        msg = self.format(msg, origin)
        return self.logger.critical(msg)

    def error(self, msg, origin=None):
        msg = self.format(msg, origin)
        return self.logger.error(msg)

    def warning(self, msg, origin=None):
        msg = self.format(msg, origin)
        return self.logger.warning(msg)

    def info(self, msg, origin=None):
        msg = self.format(msg, origin)
        return self.logger.info(msg)

    def debug(self, msg, origin=None):
        msg = self.format(msg, origin)
        return self.logger.debug(msg)

    def format(self, msg, origin=None):
        if origin:
            origin = f"{self.logger.name}.{origin} "
        else:
            origin = ""

        return f"{origin}{msg}"
