from typing import Type
import pytest
from pandas import Categorical, Series
from pandas.testing import assert_frame_equal
from datar.core.grouped import *
from datar.base import NA, mean, as_character
from datar import f
from pipda.function import FastEvalFunction


def test_instance():
    df = DataFrame({"a": [1, 2, 3]})
    gf = DataFrameGroupBy({"a": [1, 2, 3]}, _group_vars=["a"])
    assert isinstance(gf, DataFrameGroupBy)
    assert gf.equals(df)
    assert gf.attrs["_group_drop"]

    gf = DataFrameGroupBy(
        {"a": [1, 2, 3]}, _group_vars=["a"], _group_drop=False
    )
    assert not gf.attrs["_group_drop"]

    gf = DataFrameGroupBy(df, _group_vars=["a"])
    assert gf.equals(df)
    assert gf.attrs["_group_drop"]

    # group_by() nothing should be implemented in dplyr.group_by()
    # because we have to make sure it's a DataFrame, instead of a
    # DataFrameGroupBy object
    #
    # df.attrs['a'] = 1
    # gf = DataFrameGroupBy(df)
    # assert gf.equals(df)
    # assert 'a' not in gf.attrs
    # assert gf._group_vars == []


def test_group_data():
    df = DataFrame({"a": [1, 2, 3]})
    gf = DataFrameGroupBy(df, _group_vars="a")
    exp = DataFrame(
        [
            [1, [0]],
            [2, [1]],
            [3, [2]],
        ],
        columns=["a", "_rows"],
    )
    assert gf._group_data.equals(exp)

    # gf = DataFrameGroupBy(df)
    # assert gf._group_data.equals(DataFrame({"_rows": [[0, 1, 2]]}))

    # pandas bug: https://github.com/pandas-dev/pandas/issues/35202
    # df = DataFrame({"a": [1, 2, 3, NA, NA]})
    # df = DataFrame({"a": [1, 2, 3]})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    exp = DataFrame(
        # [[1, [0]], [2, [1]], [3, [2]], [NA, [3, 4]]], columns=["a", "_rows"]
        [[1, [0]], [2, [1]], [3, [2]]], columns=["a", "_rows"]
    )
    assert_frame_equal(gf._group_data, exp)


def test_group_data_cat():
    df = DataFrame({"a": Categorical([1, NA, 2], categories=[1, 2, 3])})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    exp = DataFrame(
        [
            [1, [0]],
            [2, [2]],
            # [NA, [1]],
        ],
        columns=["a", "_rows"],
    )
    # categorical kept
    exp["a"] = Categorical(exp["a"], categories=[1, 2, 3])
    assert_frame_equal(gf._group_data, exp)

    gf = DataFrameGroupBy(df, _group_vars=["a"], _group_drop=False)
    exp = DataFrame(
        [
            [1, [0]],
            [2, [2]],
            [3, []],
            # [NA, [1]],
        ],
        columns=["a", "_rows"],
    )
    exp["a"] = Categorical(exp["a"], categories=[1, 2, 3])
    assert gf._group_data.equals(exp)


def test_multi_cats():
    df = DataFrame(
        {
            "a": Categorical([1, NA, NA, NA], categories=[1, 2]),
            "b": Categorical([2, 2, 2, 3], categories=[2, 3]),
            "c": Categorical([3, NA, NA, NA], categories=[3]),
            "d": Categorical([4, NA, NA, NA], categories=[4]),
        }
    )
    gf = DataFrameGroupBy(df, _group_vars=list("abcd"), _group_drop=False)
    # assert len(gf._group_data) == 11
    assert len(gf._group_data) == 3

    gf = DataFrameGroupBy(df, _group_vars=list("abcd"), _group_drop=True)
    assert len(gf._group_data) == 3


def test_0row_df():
    df = DataFrame({"a": [], "b": []})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    assert gf._group_data.shape == (0, 2)
    assert gf._group_data.columns.tolist() == ["a", "_rows"]

    df = DataFrame({"a": Categorical([], categories=[1, 2]), "b": []})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    assert gf._group_data.shape == (0, 2)
    assert gf._group_data.columns.tolist() == ["a", "_rows"]

    gf = DataFrameGroupBy(df, _group_vars=["a"], _group_drop=False)
    assert gf._group_data.shape == (2, 2)


def test_apply():
    df = DataFrame({"a": Categorical([1, 2], categories=[1, 2, 3])})

    def n(subdf, skip=None):
        if subdf.attrs["_group_index"] == skip:
            return None
        return DataFrame(
            {
                "n": [
                    len(
                        subdf.attrs["_group_data"].loc[
                            subdf.attrs["_group_index"], "_rows"
                        ]
                    )
                ]
            }
        )

    gf = DataFrameGroupBy(df, _group_vars=["a"])
    out = gf._datar_apply(n)
    # 3 lost
    exp = DataFrame({"a": Categorical(df.a, categories=[1,2]), "n": [1, 1]})
    assert_frame_equal(out, exp)

    gf = DataFrameGroupBy(df, _group_vars=["a"], _group_drop=False)
    out = gf._datar_apply(n)
    exp = DataFrame(
        {"a": Categorical([1, 2, 3], categories=[1, 2, 3]), "n": [1, 1, 0]}
    )
    assert_frame_equal(out, exp)

    out = gf._datar_apply(n, skip=0)
    exp = DataFrame(
        {"a": Categorical([2, 3], categories=[1, 2, 3]), "n": [1, 0]}
    )
    assert_frame_equal(out, exp)

def test_agg():
    df = DataFrame(dict(a=[1,1,2,2], b=[1,2,3,4]))
    gf = DataFrameGroupBy(df, _group_vars='a')
    out = gf._datar_apply(None, _mappings=dict(
        c=f.b.mean()
    ), _method='agg')
    assert_frame_equal(
        out,
        DataFrame(dict(a=[1,2], c=[1.5,3.5]))
    )
    # numpy functions
    out = gf._datar_apply(None, _mappings=dict(
        c=FastEvalFunction(mean, args=(f.b, ), kwargs={}, dataarg=False)
    ), _method='agg')
    assert_frame_equal(
        out,
        DataFrame(dict(a=[1,2], c=[1.5,3.5]))
    )
    # numpy functions with na_rm
    out = gf._datar_apply(None, _mappings=dict(
        c=FastEvalFunction(mean, args=(f.b, ), kwargs={'na_rm': True}, dataarg=False)
    ), _method='agg')
    assert_frame_equal(
        out,
        DataFrame(dict(a=[1,2], c=[1.5,3.5]))
    )
    # fail
    with pytest.raises(TypeError, match='not callable'):
        gf._datar_apply(None, _mappings=dict(
            c=FastEvalFunction(as_character, args=(f.b, ), kwargs={}, dataarg=False)
        ), _method='agg')
    with pytest.raises(TypeError, match='not callable'):
        gf._datar_apply(None, _mappings=dict(
            c=1
        ), _method='agg')

    # no groupdata
    out = gf._datar_apply(None, _mappings=dict(
        c=f.b.mean()
    ), _method='agg', _groupdata=False)
    assert_frame_equal(
        out,
        DataFrame(dict(c=[1.5,3.5]))
    )

    # drop index
    out = gf._datar_apply(None, _mappings=dict(
        c=f.b.cummax()
    ), _method='agg', _groupdata=False)
    assert_frame_equal(
        out,
        DataFrame(dict(c=[1, 2, 3, 4]))
    )


# def test_construct_with_give_groupdata():
#     df = DataFrame({"a": [1, 2, 3]})
#     gf = DataFrameGroupBy(df, _group_vars=["a"])

#     gf2 = DataFrameGroupBy(df, _group_vars=["a"], _group_data=gf._group_data)

#     assert gf._group_data.equals(gf2._group_data)


def test_apply_returns_none():
    df = DataFrame({"a": []})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    out = gf._datar_apply(lambda df: None)
    assert out.shape == (0, 1)


def test_repr():
    df = DataFrame({"a": [1, 2, 3]})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    out = repr(gf)

    assert "[Groups: a (n=3)]" in out


def test_copy():
    df = DataFrame({"a": [1, 2, 3]})
    gf = DataFrameGroupBy(df, _group_vars=["a"])

    gf2 = gf.copy(copy_grouped=True)
    assert isinstance(gf2, DataFrameGroupBy)
    assert gf.attrs['_group_vars'] == gf2.attrs['_group_vars']
    assert gf._group_data.equals(gf2._group_data)

    gf3 = gf.copy(deep=False, copy_grouped=True)
    assert gf3.attrs['_group_vars'] is gf.attrs['_group_vars']
    assert gf3.attrs["_group_drop"] is gf.attrs["_group_drop"]
    assert gf3._group_data is gf._group_data


def test_gf_repr_html():
    df = DataFrame({"a": [1, 2, 3]})
    gf = DataFrameGroupBy(df, _group_vars=["a"])
    assert "[Groups: ['a'] (n=3)]" in gf._repr_html_()


# rowwise ---------------------------------------
def test_rowwise():
    df = DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    rf = DataFrameRowwise(df)
    assert rf._group_data.columns.tolist() == ["_rows"]
    assert rf._group_data.shape == (3, 1)
    out = rf._datar_apply(lambda df: Series({"c": df.a + df.b}))
    assert_frame_equal(out, DataFrame([[5], [7], [9]], columns=["c"]))

    rf = DataFrameRowwise(df, _group_vars=["a"])
    assert rf._group_data.columns.tolist() == ["a", "_rows"]
    assert rf._group_data.shape == (3, 2)
    out = rf._datar_apply(
        lambda df: DataFrame({"c": [df.a + df.b]}), _groupdata=True
    )
    assert_frame_equal(
        out,
        DataFrame([[1, 5], [2, 7], [3, 9]], columns=["a", "c"])
    )

    # with pytest.raises(ValueError):
    out = rf._datar_apply(lambda df: None)
    assert_frame_equal(out, df[['a']])


def test_rowwise_repr():
    df = DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    rf = DataFrameRowwise(df)
    out = repr(rf)
    assert "[Rowwise: ]" in out

    rf = DataFrameRowwise(df, _group_vars=["a"])
    out = repr(rf)
    assert "[Rowwise: a]" in out


def test_gf_repr_html():
    df = DataFrame({"a": [1, 2, 3]})
    rf = DataFrameRowwise(df, _group_vars=["a"])
    assert "Rowwise: ['a']" in rf._repr_html_()

def test_rowwise_func_returns_multirow_df():
    rf = DataFrameRowwise(DataFrame({"a": [1, 2, 3]}))
    with pytest.raises(ValueError):
        rf._datar_apply(lambda row: DataFrame({'b': [1,2]}))

def test_rowwise_default_column_prefix_used():
    rf = DataFrameRowwise(DataFrame({"a": [1, 2, 3]}))
    out = rf._datar_apply(lambda row: row.a * 2)
    assert out.columns.tolist() == ['_Var0']
