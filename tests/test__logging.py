import types

import pytest

from datareservoirio._logging import exceptions_logger, log_exception, log_retry


class my_test_class:
    def __init__(self):
        pass

    @log_exception
    @log_retry
    def divide_zero(self, num):
        return num / 0

    @log_exception
    def divide_one(self, num):
        return num / 1


@pytest.fixture
def my_class():
    my_class = my_test_class()
    my_class.logging_as_exception = False
    my_class.logging_as_warning = False
    exceptions_logger.exception = types.MethodType(change_logging, my_class)
    exceptions_logger.warning = types.MethodType(change_logging_warning, my_class)

    return my_class


def change_logging(self, msg, *args, exc_info=True, **kwargs):
    if kwargs["extra"]:
        self.logging_as_exception = True
    else:
        raise ValueError("Missing extra parameters")


def change_logging_warning(self, msg, *args, exc_info=True, **kwargs):
    if kwargs["extra"]:
        self.logging_as_warning = True
    else:
        raise ValueError("Missing extra parameters")


def test_divide_zero_is_logged(my_class):
    with pytest.raises(ZeroDivisionError):
        my_class.divide_zero(8)
    assert my_class.logging_as_exception == True


def test_divide_one_not_logged(my_class):
    my_class.divide_one(2)
    assert my_class.logging_as_exception == False
    assert my_class.logging_as_warning == False


def test_retries_is_loggged_with_criticality_2(my_class):
    with pytest.raises(ZeroDivisionError):
        my_class.divide_zero(8)
    assert my_class.logging_as_warning == True
