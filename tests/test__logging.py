import types

import pytest

from datareservoirio._logging import exceptions_logger, log_exception


class my_test_class:
    def __init__(self):
        pass

    @log_exception
    def divide_zero(self, num):
        return num / 0

    @log_exception
    def divide_one(self, num):
        return num / 1


@pytest.fixture
def my_class():
    my_class = my_test_class()
    my_class.was_called = False
    exceptions_logger.exception = types.MethodType(change_logging, my_class)
    return my_class


def change_logging(self, msg, *args, exc_info=True, **kwargs):
    if kwargs["extra"]:
        self.was_called = True
    else:
        raise ValueError("Missing extra parameters")


def test_divide_zero_is_logged(my_class):
    with pytest.raises(ZeroDivisionError):
        my_class.divide_zero(8)
    assert my_class.was_called == True


def test_divide_one_not_logged(my_class):
    my_class.divide_one(2)
    assert my_class.was_called == False
