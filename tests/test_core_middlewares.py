import pytest

from datar.core.middlewares import *
from datar.all import *

def test_inverted_repr():
    iv = Inverted('a')
    assert repr(iv) == f"Inverted(['a'])"

def test_inverted_evaluate_series():
    df = tibble(x=1)
    out = Inverted(df.x).evaluate(['x'])
    assert out == []
    out = Inverted(0).evaluate(['x'])
    assert out == []

def test_inverted_out_of_bounds():
    with pytest.raises(ColumnNotExistingError):
        Inverted(10).evaluate(['x'])
    with pytest.raises(ColumnNotExistingError):
        Inverted('y').evaluate(['x'])

def test_negated_repr():
    ng = Negated([1,2,3])
    assert repr(ng) == f"Negated([1, 2, 3])"

def test_curcolumn():
    out = CurColumn.replace_args([CurColumn()], 'cur')
    assert out == ('cur', )
