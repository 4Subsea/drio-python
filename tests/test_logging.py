import unittest
import logging

try:
    from unittest.mock import Mock
except:
    from mock import Mock

import numpy as np

import timeseriesclient
from timeseriesclient.log import LogWriter

logger = logging.getLogger(__name__)


class test_Log_Configuration(unittest.TestCase):

    def test_only_handler_is_NullHandler(self):
        logger = logging.getLogger(timeseriesclient.__name__)
        handlers = logger.handlers
        
        self.assertEqual(logger.name, 'timeseriesclient')
        self.assertEqual(len(handlers), 1)
        self.assertIsInstance(handlers[0], logging.NullHandler)

    def test_root_logger_name(self):
        self.assertEqual(timeseriesclient.logger.name, 'timeseriesclient')

    def test_root_logger_initial_level_is_warning(self):
        logger = logging.getLogger(timeseriesclient.__name__)

        self.assertEqual(logger.level, logging.WARNING)

    def test_set_logging_level(self):
        timeseriesclient.set_log_level(logging.DEBUG)

        logger = logging.getLogger(timeseriesclient.__name__)

        self.assertEqual(logger.level, logging.DEBUG)


class test_LogWriter(unittest.TestCase):

    def _verify_log_message(self, mock, msg, lvl, member):
        log_message = mock.call_args[0][0]

        parts = log_message.split(' *** ')
        self.assertEqual(parts[1], lvl)
        self.assertEqual(parts[2], member)
        self.assertEqual(parts[3], msg)

    def test_format(self):
        lw = LogWriter(logger)

        result = lw.format("ERROR", "log message", "test_format")
        results = result.split(' *** ')

        self.assertEqual(results[1], "ERROR")
        self.assertEqual(results[2], logger.name + ".test_format")
        self.assertEqual(results[3], "log message")
        self.assertIsInstance(np.datetime64(results[0]), np.datetime64) 

    def test_critical(self):
        lw = LogWriter(logger)
        logger.critical = Mock()
        lw.critical("critical message", "test_critical")
        self.assertTrue(logger.critical.called)
        self._verify_log_message(logger.critical, 
                "critical message",     
                "CRITICAL",
                logger.name + ".test_critical")

    def test_error(self):
        lw = LogWriter(logger)
        logger.error = Mock()
        lw.error("error message", "test_error")
        self.assertTrue(logger.error.called)
        self._verify_log_message(logger.error, "error message", "ERROR",
                logger.name + ".test_error")

    def test_warning(self):
        lw = LogWriter(logger)
        logger.warning = Mock()
        lw.warning("warning message", "test_warning")
        self.assertTrue(logger.warning.called)
        self._verify_log_message(logger.warning, "warning message", "WARNING",
                logger.name + ".test_warning")

    def test_info(self):
        lw = LogWriter(logger)
        logger.info = Mock()
        lw.info("info message", "test_info")
        self.assertTrue(logger.info.called)
        self._verify_log_message(logger.info, "info message", "INFO",
                logger.name + ".test_info")
    
    def test_debug(self):
        lw = LogWriter(logger)
        logger.debug = Mock()
        lw.debug("debug message", "test_debug")
        self.assertTrue(logger.debug.called)
        self._verify_log_message(logger.debug, "debug message", "DEBUG",
                logger.name + ".test_debug")
