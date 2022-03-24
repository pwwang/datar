import pytest
from datar.core.options import (
    options,
    options_context,
    add_option,
    get_option,
    OPTIONS,
)


@pytest.fixture(autouse=True)
def reset_options():
    opts = options()
    yield
    options(opts)


def test_options_empty_args_returns_full_options():
    out = options()
    assert out == OPTIONS


def test_options_with_names_only_selects_options():
    out = options("dplyr_summarise_inform")
    assert len(out) == 1
    assert out["dplyr_summarise_inform"]


def test_opts_with_names_nameval_pairs_mixed_rets_sel_opts_and_changes_option():
    out = options(dplyr_summarise_inform=False, _return=True)
    assert out == {"dplyr_summarise_inform": True}
    assert not get_option("dplyr.summarise.inform")


def test_options_with_dict_updates_options():
    out = options({"dplyr_summarise_inform": True}, _return=True)
    assert get_option("dplyr_summarise_inform")
    assert out.dplyr_summarise_inform


def test_options_context():
    assert get_option("dplyr_summarise_inform")
    with options_context(dplyr_summarise_inform=False):
        assert not get_option("dplyr_summarise_inform")

    assert get_option("dplyr_summarise_inform")


def test_add_option(capsys):
    add_option('x_y_z', default=1, callback=lambda opt: print(opt))
    assert "1" in capsys.readouterr().out

    assert get_option("x.y.z") == 1
