import pytest

from datareservoirio._logging import log_exception


class my_failing_func:
    def __init__(self):
        pass

    @log_exception
    def divide_zero(self, num):
        return num / 0


def test__divide_zero_is_logged():
    my_class = my_failing_func()

    with pytest.raises(Exception):
        my_class.divide_zero(8)
