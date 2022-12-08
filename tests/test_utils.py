import pytest
from datar.core.utils import arg_match


def test_arg_match():
    with pytest.raises(ValueError, match='abc'):
        arg_match('a', 'a', ['b', 'c'], errmsg='abc')
    with pytest.raises(ValueError, match='must be one of'):
        arg_match('a', 'a', ['b', 'c'])

    assert arg_match('a', 'a', ['a', 'b', 'c']) == 'a'
