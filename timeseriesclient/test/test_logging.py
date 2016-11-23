import unittest
import logging
import sys

sys.path.append('../../')
import timeseriesclient

class test_Log_Configuration(unittest.TestCase):

    def test_only_handler_is_NullHandler(self):
        logger = logging.getLogger(timeseriesclient.__name__)
        handlers = logger.handlers
        
        self.assertEqual(logger.name, 'timeseriesclient')
        self.assertEqual(len(handlers), 1)
        self.assertIsInstance(handlers[0], logging.NullHandler)

    def test_root_logger_initial_level_is_warning(self):
        logger = logging.getLogger(timeseriesclient.__name__)

        self.assertEqual(logger.level, logging.WARNING)

    def test_set_logging_level(self):
        timeseriesclient.set_log_level(logging.DEBUG)

        logger = logging.getLogger(timeseriesclient.__name__)

        self.assertEqual(logger.level, logging.DEBUG)
