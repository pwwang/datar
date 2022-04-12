import pytest
import os
import inspect
import builtins
import importlib
from pathlib import Path
from contextlib import contextmanager

import toml
from datar.core import options


@pytest.fixture(autouse=True)
def clear_warns():
    from datar.core import import_names_conflict

    oldopt = options("warn.builtin.names")
    options(import_names_conflict="warn")
    import_names_conflict.WARNED.clear()
    yield
    options(import_names_conflict=oldopt)


def write_options(optfile, **opts):
    with optfile.open("w") as f:
        toml.dump(opts, f)


@contextmanager
def reload_dplyr(tmpdir, **opts):
    oldcwd = Path.cwd()
    newcwd = tmpdir / "cwd"
    newcwd.mkdir()
    os.chdir(newcwd)

    configfile = newcwd / ".datar.toml"
    write_options(configfile, **opts)
    opt_module = inspect.getmodule(options)
    from datar import dplyr
    # from datar.core import import_names_conflict

    importlib.reload(opt_module)
    # importlib.reload(import_names_conflict)
    importlib.reload(dplyr)
    try:
        yield
    finally:
        os.chdir(oldcwd)
        importlib.reload(opt_module)
        importlib.reload(dplyr)


def test_nonexist_names_donot_exist():
    with pytest.raises(ImportError):
        from datar.dplyr import x

    with pytest.raises(ImportError):
        from datar.all import x


def test_trailing_underscore_bypass_warn(caplog):
    from datar.dplyr import filter_

    assert callable(filter_)
    assert filter_ is not builtins.filter
    assert caplog.text == ""


def test_direct_import_gets_warning(caplog):
    from datar.dplyr import filter

    assert '"filter" has been masked' in caplog.text
    caplog.clear()

    # second import won't warn
    from datar.dplyr import filter

    assert caplog.text == ""


def test_direct_import_from_all_warns_once(caplog):
    from datar.all import slice

    assert caplog.text.count('"slice"') == 1


def test_config_file_controls_silent(caplog, tmp_path):

    with reload_dplyr(tmp_path, import_names_conflict="silent"):
        from datar.dplyr import slice

        assert caplog.text == ""


def test_config_file_controls_underscore_suffixed(tmp_path):
    with reload_dplyr(tmp_path, import_names_conflict="underscore_suffixed"):

        # from datar.dplyr import slice_

        with pytest.raises(ImportError):
            from datar.dplyr import slice
