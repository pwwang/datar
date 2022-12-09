import pytest
from datar.core.options import (
    options,
    options_context,
    add_option,
    get_option,
)


@pytest.fixture(autouse=True)
def reset_options():
    opts = options()
    add_option("x_y_z", True)
    yield
    options(opts)


def test_options_empty_args_returns_full_options():
    from datar.core.options import OPTIONS
    out = options()
    assert out == OPTIONS


def test_options_with_names_only_selects_options():
    out = options("x_y_z")
    assert len(out) == 1
    assert out["x_y_z"]


def test_opts_with_names_nameval_pairs_mixed_rets_sel_opts_and_changes_option():
    out = options(x_y_z=False, _return=True)
    assert out == {"x_y_z": True}
    assert not get_option("x.y.z")


def test_options_with_dict_updates_options():
    out = options({"x_y_z": True}, _return=True)
    assert get_option("x_y_z")
    assert out.x_y_z


def test_options_context():
    assert get_option("x_y_z")
    with options_context(x_y_z=False):
        assert not get_option("x_y_z")

    assert get_option("x_y_z")
