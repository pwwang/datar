import pytest
from plyrda.utils import *

@pytest.mark.parametrize('collections, expects', [
    (1, [1]),
    ([1], [1]),
    ([[1]], [1]),
])
def test_expand_collections(collections, expects):
    assert expand_collections(collections) == expects

def test_unaryneg():
    x = UnaryNeg('a', DataFrame({'a': [1], 'b': [2]}))
    assert x.elems == Collection('a')
    assert x.complements == ['b']


@pytest.mark.parametrize('all_columns,match,ignore_case,func,expects', [
    (['a1', 'a2', 'b1', 'b2'],
     'a',
     True,
     lambda mat, cname: cname.startswith(mat),
     ['a1', 'a2']),
    (['a1', 'a2', 'b1', 'b2', 'c3'],
     ['1', '2'],
     True,
     lambda mat, cname: cname.endswith(mat),
     ['a1', 'b1', 'a2', 'b2']),
])
def test_filter_columns(all_columns, match, ignore_case, func, expects):
    assert filter_columns(all_columns, match, ignore_case, func) == expects

def test_list_diff():
    assert list_diff([3,1,2],[2]) == [3,1]

def test_list_intersect():
    assert list_intersect([3,1,2],[2]) == [2]

def test_check_column():
    with pytest.raises(ColumnNameInvalidError):
        check_column({1: 2})

@pytest.mark.parametrize('all_columns, columns, expects', [
    (['a', 'b'], ('a', ), ['a']),
    (['a', 'b'], (0, ), ['a']),
    (['a', 'b'], (['b'], ), ['b']),
    (['a', 'b'], (UnaryNeg('a', DataFrame({'a':[1], 'b':[2]})), ), ['b']),
])
def test_select_columns(all_columns, columns, expects):
    assert select_columns(all_columns, *columns, raise_nonexist=False) == expects

def test_select_columns_error():
    with pytest.raises(ColumnNameInvalidError):
        select_columns(["a", "b"], "a", UnaryNeg("b", None))
    with pytest.raises(ColumnNotExistingError):
        select_columns(["a", "b"], "c")
