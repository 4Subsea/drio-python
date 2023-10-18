import pytest

from datareservoirio._logging import log_exception


class my_test_class:
    def __init__(self):
        pass

    @log_exception
    def divide_zero(self, num):
        return num / 0
    
    @log_exception
    def divide_one(self, num):
        return num / 1


def test__divide_zero_is_logged():
    my_class = my_test_class()

    with pytest.raises(Exception):
        my_class.divide_zero(8)
