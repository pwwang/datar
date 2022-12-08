import pytest
import os
import sys
import builtins
from subprocess import Popen, PIPE

import rtoml
from datar import options


@pytest.fixture(autouse=True)
def clear_warns():
    from datar.core import import_names_conflict

    oldopt = options("warn.builtin.names")
    options(import_names_conflict="warn")
    import_names_conflict.WARNED.clear()
    yield
    options(import_names_conflict=oldopt)


def _setup_process(tmpdir, script, **opts):

    newcwd = tmpdir / "cwd"
    newcwd.mkdir()
    os.chdir(newcwd)

    configfile = newcwd / ".datar.toml"
    write_options(configfile, **opts)

    scriptfile = newcwd / "script.py"
    scriptfile.write_text(script)

    return scriptfile


def write_options(optfile, **opts):
    with optfile.open("w") as f:
        rtoml.dump(opts, f)


def test_nonexist_names_donot_exist():
    with pytest.raises(ImportError):
        from datar.dplyr import x

    with pytest.raises(ImportError):
        from datar.all import x  # noqa: F401, F811


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
    from datar.dplyr import filter  # noqa: F401, F811

    assert caplog.text == ""


def test_direct_import_from_all_warns_once(caplog):
    from datar.all import slice  # noqa: F401

    assert caplog.text.count('"slice"') == 1


def test_config_file_controls_silent(tmp_path):

    script = _setup_process(
        tmp_path,
        "from datar.dplyr import slice",
        import_names_conflict="silent",
    )

    out, err = Popen(
        [sys.executable, script],
        stderr=PIPE,
        stdout=PIPE,
        encoding="utf-8",
    ).communicate()
    assert out == ""
    assert err == ""


def test_config_file_controls_underscore_suffixed(tmp_path):

    script = _setup_process(
        tmp_path,
        "from datar.dplyr import slice",
        import_names_conflict="underscore_suffixed",
    )

    out, err = Popen(
        [sys.executable, script],
        stderr=PIPE,
        stdout=PIPE,
        encoding="utf-8",
    ).communicate()
    assert out == ""
    assert "ImportError" in err
