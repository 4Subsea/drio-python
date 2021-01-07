import logging
import unittest

import datareservoirio

logger = logging.getLogger(__name__)


class test_Log_Configuration(unittest.TestCase):
    def test_only_handler_is_NullHandler(self):
        logger = logging.getLogger(datareservoirio.__name__)
        handlers = logger.handlers

        self.assertEqual(logger.name, "datareservoirio")
        self.assertEqual(len(handlers), 1)
        self.assertIsInstance(handlers[0], logging.NullHandler)

    def test_root_logger_name(self):
        self.assertEqual(datareservoirio.logger.name, "datareservoirio")

    def test_root_logger_initial_level_is_warning(self):
        logger = logging.getLogger(datareservoirio.__name__)

        self.assertEqual(logger.level, logging.WARNING)

    def test_set_logging_level(self):
        datareservoirio.set_log_level(logging.DEBUG)

        logger = logging.getLogger(datareservoirio.__name__)

        self.assertEqual(logger.level, logging.DEBUG)


if __name__ == "__main__":
    unittest.main()
