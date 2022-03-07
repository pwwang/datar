import pytest
import builtins
from datar.core.options import options

@pytest.fixture(autouse=True)
def clear_warns():
    from datar.core import warn_builtin_names
    oldopt = options('warn.builtin.names')
    options(warn_builtin_names=True)
    warn_builtin_names.WARNED.clear()
    yield
    options(warn_builtin_names=oldopt)

def test_nonexist_names_donot_exist():
    with pytest.raises(ImportError):
        from datar.dplyr import x

    with pytest.raises(ImportError):
        from datar.all import x

def test_trailing_underscore_bypass_warn(caplog):
    from datar.dplyr import filter_
    assert callable(filter_)
    assert filter_ is not builtins.filter
    assert caplog.text == ''

def test_direct_import_gets_warning(caplog):
    from datar.dplyr import filter
    assert '"filter" has been overriden' in caplog.text
    caplog.clear()

    # second import won't warn
    from datar.dplyr import filter
    assert caplog.text == ''

def test_direct_import_from_all_warns_once(caplog):
    from datar.all import slice
    assert caplog.text.count('"slice"') == 1
