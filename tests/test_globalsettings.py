import datareservoirio as drio


def test_environment():
    assert isinstance(drio.globalsettings.environment, drio.environments.Environment)
