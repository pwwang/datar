import warnings

import pytest
from pipda import register_function
from plyrda.funcs import *
from plyrda.data import iris

data = DataFrame({
    'a1': [1],
    'a2': [2],
    'b1': [3],
    'b2': [4],
    'c3': [5],
})

@pytest.fixture(autouse=True)
def no_warnings():
    warnings.simplefilter('ignore')

@pytest.mark.parametrize('df, match, ignore_case, vars, expects', [
    (data, 'a', False, None, ['a1', 'a2']),
    (data, ['a', 'B'], False, None, ['a1', 'a2']),
    (data, ['a', 'B'], True, None, ['a1', 'a2', 'b1', 'b2']),
    (data, ['a', 'B'], True, ['a3', 'b3'], ['a3', 'b3']),
])
def test_starts_with(df, match, ignore_case, vars, expects):
    assert starts_with(df, match, ignore_case, vars) == expects


@pytest.mark.parametrize('df, match, ignore_case, vars, expects', [
    (data, '1', False, None, ['a1', 'b1']),
    (data, ['1', '2'], False, None, ['a1', 'b1', 'a2', 'b2']),
    (data, ['1', '2'], True, ['c1', 'c2'], ['c1', 'c2']),
])
def test_ends_with(df, match, ignore_case, vars, expects):
    assert ends_with(df, match, ignore_case, vars) == expects

@pytest.mark.parametrize('df, match, ignore_case, vars, expects', [
    (data, 'a', False, None, ['a1', 'a2']),
    (data, ['a', 'B'], False, None, ['a1', 'a2']),
    (data, ['a', 'B'], True, None, ['a1', 'a2', 'b1', 'b2']),
    (data, ['a', 'B'], True, ['1a3', 'cb3'], ['1a3', 'cb3']),
])
def test_contains(df, match, ignore_case, vars, expects):
    assert contains(df, match, ignore_case, vars) == expects

@pytest.mark.parametrize('df, match, ignore_case, vars, expects', [
    (data, 'a', False, None, ['a1', 'a2']),
    (data, ['a', 'B'], False, None, ['a1', 'a2']),
    (data, ['a', 'B'], True, None, ['a1', 'a2', 'b1', 'b2']),
    (data, r'\d.3', True, ['1a3', 'cb3'], ['1a3']),
])
def test_matches(df, match, ignore_case, vars, expects):
    assert matches(df, match, ignore_case, vars) == expects

@pytest.mark.parametrize('df, prefix, range, width, expects', [
    (data, 'a', range(3), 2, ['a00', 'a01', 'a02']),
])
def test_num_range(df, prefix, range, width, expects):
    assert num_range(df, prefix, range, width) == expects

@pytest.mark.parametrize('df, expects', [
    (data, ['a1', 'a2', 'b1', 'b2', 'c3']),
])
def test_everything(df, expects):
    assert everything(df) == expects

@pytest.mark.parametrize('df, offset, vars, expects', [
    (data, 2, None, 'b1'),
    (data, 0, None, 'c3'),
    (data, 1, ['x', 'y', 'z'], 'y'),
])
def test_last_col(df, offset, vars, expects):
    assert last_col(df, offset, vars) == expects

@pytest.mark.parametrize('df, args, expects', [
    (data, (2, ), [2]),
    (data, ([2], ), [2]),
    (data, ([[2]], ), [2]),
    (data, ([[2]], 3), [2, 3]),
])
def test_c(df, args, expects):
    assert c(df, *args) == expects

def test_all_of():

    assert all_of(data, ['a1', 'a2']) == ['a1', 'a2']
    with pytest.raises(ColumnNotExistingError):
        all_of(data, ['x', 'a1'])

def test_any_of():

    assert any_of(data, ['x', 'a1']) == ['a1']
    assert any_of(data, ['x', 'y']) == []

def test_where():
    from plyrda.commons import is_numeric
    numeric_cols = where(iris, is_numeric)
    assert numeric_cols == [
        'Sepal.Length', 'Sepal.Width', 'Petal.Length', 'Petal.Width'
    ]

    @register_function
    def return_true(data, x):
        return True

    numeric_cols = where(iris, return_true)
    assert numeric_cols == [
        'Sepal.Length', 'Sepal.Width', 'Petal.Length', 'Petal.Width', 'Species'
    ]

    def plain_return_true(x):
        return True
    numeric_cols = where(iris, plain_return_true)
    assert numeric_cols == [
        'Sepal.Length', 'Sepal.Width', 'Petal.Length', 'Petal.Width', 'Species'
    ]
    numeric_cols = where(iris, lambda x: False)
    assert numeric_cols == []
