import sys
import subprocess
from pathlib import Path

import pytest


def _run_conflict_names(module, allow_conflict_names, getat, error):
    here = Path(__file__).parent
    conflict_names = here / "conflict_names.py"
    cmd = [
        sys.executable,
        str(conflict_names),
        "--module",
        module,
    ]
    if error:
        cmd += ["--error", error]
    if allow_conflict_names:
        cmd.append("--allow-conflict-names")
    if getat:
        cmd.append("--getattr")

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.wait(), " ".join(cmd)


def test_from_all_import_allow_conflict_names_true():
    r, cmd = _run_conflict_names("all", True, False, None)
    assert r == 0, cmd


def test_from_all_import_allow_conflict_names_false():
    r, cmd = _run_conflict_names("all", False, False, "ImportError")
    assert r == 0, cmd


def test_all_getattr_allow_conflict_names_true():
    r, cmd = _run_conflict_names("all", True, True, None)
    assert r == 0, cmd


def test_all_getattr_allow_conflict_names_false():
    r, cmd = _run_conflict_names("all", False, True, None)
    assert r == 0, cmd


def test_from_base_import_allow_conflict_names_true():
    r, cmd = _run_conflict_names("base", True, False, None)
    assert r == 0, cmd


def test_from_base_import_allow_conflict_names_false():
    r, cmd = _run_conflict_names("base", False, False, "ImportError")
    assert r == 0, cmd


def test_base_getattr_allow_conflict_names_true():
    r, cmd = _run_conflict_names("base", True, True, None)
    assert r == 0, cmd


def test_base_getattr_allow_conflict_names_false():
    r, cmd = _run_conflict_names("base", False, True, None)
    assert r == 0, cmd


def test_from_dplyr_import_allow_conflict_names_true():
    r, cmd = _run_conflict_names("dplyr", True, False, None)
    assert r == 0, cmd


def test_from_dplyr_import_allow_conflict_names_false():
    r, cmd = _run_conflict_names("dplyr", False, False, "ImportError")
    assert r == 0, cmd


def test_dplyr_getattr_allow_conflict_names_true():
    r, cmd = _run_conflict_names("dplyr", True, True, None)
    assert r == 0, cmd


def test_dplyr_getattr_allow_conflict_names_false():
    r, cmd = _run_conflict_names("dplyr", False, True, None)
    assert r == 0, cmd
