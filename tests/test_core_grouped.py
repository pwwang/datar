from pandas.core.arrays.categorical import Categorical
import pytest
from datar.core.grouped import *

def test_instance():
    df = DataFrame({'a': [1,2,3]})
    gf = DataFrameGroupBy({'a': [1,2,3]}, _group_vars=['a'])
    assert isinstance(gf, DataFrameGroupBy)
    assert gf.equals(df)
    assert gf.attrs['groupby_drop']

    gf = DataFrameGroupBy({'a': [1,2,3]}, _group_vars=['a'], _drop=False)
    assert not gf.attrs['groupby_drop']

    gf = DataFrameGroupBy(df, _group_vars=['a'])
    assert gf.equals(df)
    assert gf.attrs['groupby_drop']

    df.attrs['a'] = 1
    gf = DataFrameGroupBy(df)
    assert gf.equals(df)
    assert 'a' not in gf.attrs
    assert gf._group_vars == []

def test_group_data():
    df = DataFrame({'a': [1,2,3]})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    exp = DataFrame([
        [1, [0]],
        [2, [1]],
        [3, [2]],
    ], columns=['a', '_rows'])
    assert gf._group_data.equals(exp)

    gf = DataFrameGroupBy(df)
    assert gf._group_data.equals(DataFrame({'_rows': [[0,1,2]]}))

def test_group_data_cat():
    df = DataFrame({'a': Categorical([1,NA,2], categories=[1,2,3])})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    exp = DataFrame([
        [1.0, [0]],
        [2.0, [2]],
        [NA, [1]],
    ], columns=['a', '_rows'])
    # categorical kept
    exp['a'] = Categorical(exp['a'], categories=[1,2,3])
    assert gf._group_data.equals(exp)

    gf = DataFrameGroupBy(df, _group_vars=['a'], _drop=False)
    exp = DataFrame([
        [1.0, [0]],
        [2.0, [2]],
        [3.0, []],
        [NA, [1]],
    ], columns=['a', '_rows'])
    exp['a'] = Categorical(exp['a'], categories=[1,2,3])
    assert gf._group_data.equals(exp)

def test_multi_cats():
    df = DataFrame({
        'a': Categorical([1, NA, NA], categories=[1]),
        'b': Categorical([2, 2, 3], categories=[2,3]),
        'c': Categorical([3, None, None], categories=[3]),
        'd': Categorical([4, NA, NA], categories=[4]),
    })
    gf = DataFrameGroupBy(df, _group_vars=list('abcd'), _drop=False)
    assert len(gf._group_data) == 8

def test_0row_df():
    df = DataFrame({'a': [], 'b': []})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    assert gf._group_data.shape == (0, 2)
    assert gf._group_data.columns.tolist() == ['a', '_rows']

    df = DataFrame({'a': Categorical([], categories=[1,2]), 'b': []})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    assert gf._group_data.shape == (0, 2)
    assert gf._group_data.columns.tolist() == ['a', '_rows']

    gf = DataFrameGroupBy(df, _group_vars=['a'], _drop=False)
    assert gf._group_data.shape == (2, 2)

def test_apply():
    df = DataFrame({'a': Categorical([1,2], categories=[1,2,3])})

    def n(subdf, skip=None):
        if subdf.attrs['group_index'] == skip:
            return None
        return DataFrame({"n": [len(
            subdf.attrs['group_data'].loc[subdf.attrs['group_index'], '_rows']
        )]})

    gf = DataFrameGroupBy(df, _group_vars=['a'])
    out = gf.group_apply(n)
    exp = DataFrame({
        'a': df.a,
        'n': [1,1]
    })
    assert out.equals(exp)

    gf = DataFrameGroupBy(df, _group_vars=['a'], _drop=False)
    out = gf.group_apply(n)
    exp = DataFrame({
        'a': Categorical([1,2,3], categories=[1,2,3]),
        'n': [1,1,0]
    })
    assert out.equals(exp)

    out = gf.group_apply(n, skip=0)
    exp = DataFrame({
        'a': Categorical([2,3], categories=[1,2,3]),
        'n': [1,0]
    })
    assert out.equals(exp)