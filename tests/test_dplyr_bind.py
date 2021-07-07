# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-bind.R
import pytest

from pandas.testing import assert_frame_equal
from datar.all import *

def test_handle_dict():
    expect = tibble(x = 1, y = "a", z = 2)
    d1 = dict(x=1, y="a")
    d2 = dict(z=2)

    out = bind_cols(d1, d2)
    assert out.equals(expect)

    out = bind_cols(dict(**d1, **d2))
    assert out.equals(expect)

def test_empty():

    out = bind_cols(tibble())
    expect = tibble()
    assert out.equals(expect)

def test_all_null():
    out = bind_cols(dict(a=NULL, b=NULL))
    expect = tibble()
    assert out.equals(expect)

    out = bind_cols(NULL)
    expect = tibble()
    assert out.equals(expect)

def test_bind_col_null():
    df1 = tibble(a = range(1,11), b = range(1,11))
    df2 = tibble(c = range(1,11), d = range(1,11))

    res1 = df1 >> bind_cols(df2)
    res2 = NULL >> bind_cols(df1, df2)
    res3 = df1 >> bind_cols(NULL, df2)
    res4 = df1 >> bind_cols(df2, NULL)

    assert res1.equals(res2)
    assert res1.equals(res3)
    assert res1.equals(res4)

def test_repair_names():
    df = tibble(a = 1, b = 2)
    bound = bind_cols(df, df, base0_=True)
    assert bound.columns.tolist() == ['a__0', 'b__1', 'a__2', 'b__3']

    t1 = tibble(a=1)
    t2 = tibble(a=2)
    bound = bind_cols(t1, t2, base0_=True)
    assert bound.columns.tolist() == ['a__0', 'a__1']

def test_incompatible_size_fill_with_NA():
    df1 = tibble(x=range(1,4))
    df2 = tibble(y=range(1,2))
    out = (df1 >> bind_cols(df2)).fillna(100)
    assert out.x.tolist() == [1,2,3]
    assert out.y.tolist() == [1,100,100]


# bind_rows

def test_reorder_cols():
    df = tibble(
        a=1,
        b=2,
        c=3,
        d=4,
        e=5,
        f=6
    )
    df_scramble = df[sample(df.columns)]
    out = df >> bind_rows(df_scramble)
    assert out.columns.tolist() == list('abcdef')

def test_ignores_null_empty():
    df = tibble(a=1)
    out = df >> bind_rows(NULL)
    assert out.equals(df)

    df0 = tibble()
    out = df >> bind_rows(df0)
    assert out.equals(df)

    # no rows
    df_no_rows = df.iloc[[], :]
    out = df >> bind_rows(df_no_rows)
    assert out.equals(df)

    # no cols
    df_no_cols = df.iloc[:, []]
    out = df >> bind_rows(df_no_cols)
    rows = out >> nrow()
    assert rows == 2

    val = out.fillna(1234) >> get(1, f.a)
    assert val == 1234

    out = df_no_cols >> bind_rows(df)
    rows = out >> nrow()
    assert rows == 2

    val = out.fillna(888) >> get(0, f.a)
    assert val == 888

# column coercion
def test_int_to_float():
    df1 = tibble(a=1.0, b=2)
    df2 = tibble(a=1, b=2)
    out = df1 >> bind_rows(df2)
    a_type = is_float(out.a)
    assert a_type
    b_type = is_int(out.b)
    assert b_type

def test_factor_to_chars():
    # we don't have warnings
    df1 = tibble(a = factor("a"))
    df2 = tibble(a = "b")

    out = df1 >> bind_rows(df1, df2)
    a_type = is_factor(out.a)
    assert not a_type

def test_bind_factors():
    df1 = tibble(a = factor("a"))
    df2 = tibble(a = factor("b"))

    out = df1 >> bind_rows(df2)
    assert out.a.cat.categories.tolist() == ["a", "b"]

    df1 = tibble(a = factor("a"))
    df2 = tibble(a = factor(NA))

    out = df1 >> bind_rows(df2)
    assert out.a.cat.categories.tolist() == ["a"]
    assert out.a.astype(object).fillna("NA").tolist() == ["a", "NA"]

    out2 = None >> bind_rows([df1, df2])
    assert_frame_equal(out2, out)

def test_bind_na_cols():
    df1 = tibble(x=factor(["foo", "bar"]))
    df2 = tibble(x=NA)

    out = df1 >> bind_rows(df2)
    res = out >> get(2, f.x)
    y = is_na(res)
    assert y

    out = df2 >> bind_rows(df1)
    res = out >> get(0, f.x)
    y = is_na(res)
    assert y

    y = is_categorical(out.x)
    assert y

def test_complex():
    df1 = tibble(r=[1+1j, 2-1j])
    df2 = tibble(r=[1-1j, 2+1j])
    df3 = df1 >> bind_rows(df2)
    out = df3 >> nrow()
    assert out == 4
    assert df3.r.tolist() == df1.r.tolist() + df2.r.tolist()

def test_cat_ordered():
    df = tibble(x=factor([1,2,3], ordered=True))
    y = bind_rows(df, df)
    assert y.x.cat.ordered

def test_create_id_col():
    df = tibble(x=range(1,11))
    df1 = df >> head(3)
    df2 = df >> tail(2)
    out = df1 >> bind_rows(df2, _id='col')
    assert out.col.tolist() == [1,1,1,2,2]

    out = bind_rows([df1, df2], _id='col', base0_=True)
    assert out.col.tolist() == [0,0,0,1,1]

    out = bind_rows(None, one=df1, two=df2, _id="col")
    assert out.col.tolist() == ['one'] * 3 + ['two'] * 2

def test_non_existing_col():
    # fill with NA, but not convert whole column to NAs
    df1 = tibble(x=letters)
    df2 = tibble(x=letters[:10], y=letters[:10])
    out = df1 >> bind_rows(df2)
    assert not out.y.isna().all()

def test_empty_dict():
    df = bind_rows({})
    d = df >> dim()
    assert d == (0, 0)

def test_rowwise_vector():
    tbl = tibble(a = "foo", b = "bar") >> bind_rows(
        dict(a = "A", b = "B")
    )
    expect = tibble(a=["foo", "A"], b=["bar", "B"])
    assert tbl.equals(expect)

    id_tbl = bind_rows(None, a=dict(a=1, b=2), b=dict(a=3, b=4), _id="id")
    expect = tibble(id=['a', 'b'], a=[1,3], b=[2,4])
    assert id_tbl.equals(expect)

def test_list_as_first_argument():
    ll = tibble(a = 1, b = 2)
    out = bind_rows([ll])
    assert out.equals(ll)

    out = bind_rows([ll, ll])
    expect = tibble(a=[1,1], b=[2,2])
    assert out.equals(expect)

def test_hierachical_data():
    my_list = [dict(x = 1, y = "a"), dict(x = 2, y = "b")]
    res = my_list >> bind_rows()
    rows = nrow(res)
    assert rows == 2
    out = is_int(res.x)
    assert out
    out = is_character(res.y)
    assert out

    res = dict(x = 1, y = "a") >> bind_rows(dict(x = 2, y = "b"))
    rows = nrow(res)
    assert rows == 2
    out = is_int(res.x)
    assert out
    out = is_character(res.y)
    assert out

# vectors
# keyword arguments have to have dict/dataframe
# it is not working like tibble
# for example: bind_rows(a=[1,2]) is not working

def test_wrong_first_argument():
    with pytest.raises(NotImplementedError):
        1 >> bind_rows()

def test_errors():
    df1 = tibble(x = [1,2,3])
    df2 = tibble(x = [4,5,6])
    with pytest.raises(ValueError):
        df1 >> bind_rows(df2, _id=5)

    df1 = tibble(a = factor("a"))
    df2 = tibble(a = 1)
    df1 >> bind_rows(df2) # no error, all converted to object

    with pytest.raises(TypeError):
        [1,2] >> bind_rows()

# for coverage
def test_bind_empty_dfs():
    out = bind_rows(None)
    assert dim(out) == (0, 0)

    out = bind_cols(None)
    assert dim(out) == (0, 0)

    df1 = tibble(x=factor([1,2,3]))
    df2 = tibble()
    out = df1 >> bind_rows(df2)
    assert out.x.tolist() == [1,2,3]
