import pytest
from datar.core.options import *

@pytest.fixture(autouse=True)
def reset_options():
    opts = options()
    yield
    options(opts)

def test_options_empty_args_returns_full_options():
    out = options()
    assert out == OPTIONS

def test_options_with_names_only_selects_options():
    out = options('index.base.0')
    assert len(out) == 1
    assert not out['index.base.0']

def test_options_with_names_name_value_pairs_mixed_returns_select_options_and_changes_option():
    out = options('index.base.0', dplyr_summarise_inform=False)
    assert out == {'index_base_0': False, 'dplyr_summarise_inform': True}
    assert not get_option('dplyr.summarise.inform')

def test_options_with_dict_updates_options():
    out = options({'index.base.0': True})
    assert get_option('index.base.0')
    assert not out.index_base_0

def test_options_context():
    assert not get_option('index.base.0')
    with options_context(index_base_0=True):
        assert get_option('index.base.0')

    assert not get_option('index.base.0')
