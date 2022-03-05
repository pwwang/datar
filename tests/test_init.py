import pytest  # noqa
from datar import get_versions, _VersionsTuple, __version__

def test_get_versions(capsys):
    vers = get_versions(False)
    assert isinstance(vers, _VersionsTuple)
    assert vers.datar == __version__

    get_versions(True)
    assert __version__ in capsys.readouterr().out
