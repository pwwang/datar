import pytest  # noqa
from datar import get_versions, __version__

def test_get_versions(capsys):
    vers = get_versions(False)
    assert isinstance(vers, dict)
    assert vers['datar'] == __version__

    get_versions(True)
    assert __version__ in capsys.readouterr().out
