import types

import pytest

from datareservoirio._logging import exceptions_logger, log_exception

import os


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
        if os.getenv("ENGINE_ROOM_APP_ID") is not None:
            self.engine_room_app_id = kwargs["extra"]["custom_dimensions"][
                "EngineRoom App ID"
            ]
    else:
        raise ValueError("Missing extra parameters")


def test_divide_zero_is_logged(my_class):
    with pytest.raises(ZeroDivisionError):
        my_class.divide_zero(8)
    assert my_class.was_called == True


def test_divide_one_not_logged(my_class):
    my_class.divide_one(2)
    assert my_class.was_called == False


def test_engine_room_app_id_is_logged(my_class, monkeypatch):
    monkeypatch.setenv("ENGINE_ROOM_APP_ID", "123")
    with pytest.raises(ZeroDivisionError):
        my_class.divide_zero(8)
    assert my_class.engine_room_app_id == "123"
