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

    df = DataFrame({'a': [1,2,3,NA,NA]})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    exp = DataFrame([
        [1, [0]],
        [2, [1]],
        [3, [2]],
        [NA,[3,4]]
    ], columns=['a', '_rows'])
    assert gf._group_data.equals(exp)

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
        'a': Categorical([1, None, NA, NA], categories=[1]),
        'b': Categorical([2, 2, 2, 3], categories=[2,3]),
        'c': Categorical([3, None, NA, None], categories=[3]),
        'd': Categorical([4, NA, None, NA], categories=[4]),
    })
    gf = DataFrameGroupBy(df, _group_vars=list('abcd'), _drop=False)
    assert len(gf._group_data) == 8

    gf = DataFrameGroupBy(df, _group_vars=list('abcd'), _drop=True)
    assert len(gf._group_data) == 3

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

def test_construct_with_give_groupdata():
    df = DataFrame({'a': [1,2,3]})
    gf = DataFrameGroupBy(df, _group_vars=['a'])

    gf2 = DataFrameGroupBy(df, _group_vars=['a'], _group_data=gf._group_data)

    assert gf._group_data.equals(gf2._group_data)

def test_apply_returns_none():
    df = DataFrame({'a': []})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    out = gf.group_apply(lambda df: None)
    assert out is None

def test_repr():
    df = DataFrame({'a': [1,2,3]})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    out = repr(gf)

    assert "[Groups: ['a'] (n=3)]" in out

def test_copy():
    df = DataFrame({'a': [1,2,3]})
    gf = DataFrameGroupBy(df, _group_vars=['a'])

    gf2 = gf.copy()
    assert isinstance(gf2, DataFrameGroupBy)
    assert gf._group_vars == gf2._group_vars
    assert gf._group_data.equals(gf2._group_data)

def test_gf_repr_html():
    df = DataFrame({'a': [1,2,3]})
    gf = DataFrameGroupBy(df, _group_vars=['a'])
    assert "[Groups: ['a'] (n=3)]" in gf._repr_html_()

# rowwise ---------------------------------------
def test_rowwise():
    df = DataFrame({'a': [1,2,3], 'b': [4,5,6]})
    rf = DataFrameRowwise(df)
    assert rf._group_data.columns.tolist() == ['_rows']
    assert rf._group_data.shape == (3, 1)
    out = rf.group_apply(
        lambda df: DataFrame({'c': df.a + df.b})
    )
    assert out.equals(DataFrame([[5],[7],[9]], columns=['c']))

    rf = DataFrameRowwise(df, _group_vars=['a'])
    assert rf._group_data.columns.tolist() == ['a', '_rows']
    assert rf._group_data.shape == (3, 2)
    out = rf.group_apply(
        lambda df: DataFrame({'c': df.a + df.b}),
        _groupdata=True
    )
    assert out.equals(DataFrame([[1,5],[2,7],[3,9]], columns=['a', 'c']))

    with pytest.raises(ValueError):
        rf.group_apply(lambda df: None, _spike_groupdata=DataFrame())

def test_rowwise_repr():
    df = DataFrame({'a': [1,2,3], 'b': [4,5,6]})
    rf = DataFrameRowwise(df)
    out = repr(rf)
    assert '[Rowwise: []]' in out

    rf = DataFrameRowwise(df, _group_vars=['a'])
    out = repr(rf)
    assert "[Rowwise: ['a']]" in out

def test_gf_repr_html():
    df = DataFrame({'a': [1,2,3]})
    rf = DataFrameRowwise(df, _group_vars=['a'])
    assert "[Rowwise: ['a'] (n=3)]" in rf._repr_html_()
