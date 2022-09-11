# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-summarise.r

import pytest
from datar import f
from datar.datasets import mtcars
from datar.core.contexts import Context
from datar.core.exceptions import NameNonUniqueError
from datar.core.tibble import TibbleRowwise
from datar.base import runif, quantile, rnorm
from datar.base import c, mean, rep, seq_len, dim, letters, sum, length
from datar.dplyr import (
    summarise,
    summarize,
    group_by,
    group_vars,
    mutate,
    group_rows,
    ends_with,
    rowwise,
    across,
)
from datar.tibble import tibble
from datar.testing import assert_tibble_equal
from pipda import register_func

from ..conftest import assert_iterable_equal


def test_freshly_create_vars():
    df = tibble(x=range(1, 11))
    out = summarise(df, y=mean(f.x), z=f.y + 1)
    assert out.y.to_list() == [5.5]
    assert out.z.to_list() == [6.5]


def test_input_recycled():
    df1 = tibble() >> summarise(x=1, y=[1, 2, 3], z=1)
    df2 = tibble(x=1, y=[1, 2, 3], z=1)
    assert df1.equals(df2)

    gf = group_by(tibble(a=[1, 2]), f.a)
    df1 = gf >> summarise(x=1, y=[1, 2, 3], z=1)
    df2 = tibble(
        a=rep([1, 2], each=3), x=1, y=rep([1, 2, 3], 2), z=1
    ) >> group_by(f.a)
    assert_tibble_equal(df1, df2)

    df1 = gf >> summarise(x=seq_len(f.a), y=1)
    df2 = tibble(a=c(1, 2, 2), x=c(1, 1, 2), y=1) >> group_by(f.a)
    # assert df1.equals(df2)
    assert_tibble_equal(df1, df2)


def test_works_with_empty_data_frames():
    df = tibble(x=[])
    df1 = summarise(df)
    df2 = tibble(_rows=1)
    assert df1.equals(df2)

    df = tibble(_rows=10)
    df1 = summarise(df)
    assert df1.equals(df2)

    # df1 = df >> summarise(n=n())
    # df2 = tibble(n=10)
    # assert df1.equals(df2)


def test_works_with_grouped_empty_data_frames():
    df = tibble(x=[])
    df1 = df >> group_by(f.x) >> summarise(y=1)
    assert dim(df1) == (0, 2)
    assert df1.columns.tolist() == ["x", "y"]

    df1 = df >> rowwise(f.x) >> summarise(y=1)
    assert group_vars(df1) == ["x"]
    assert dim(df1) == (0, 2)
    assert df1.columns.tolist() == ["x", "y"]


def test_no_expressions():
    df = tibble(x=[1, 2], y=[1, 2])
    gf = group_by(df, f.x)

    out = summarise(df)
    assert dim(out) == (1, 0)

    out = summarise(gf)
    assert group_vars(out) == []
    exp = tibble(x=[1, 2])
    assert_tibble_equal(out, exp)

    out = summarise(df, tibble())
    assert dim(out) == (1, 0)

    out = summarise(gf, tibble())
    assert group_vars(out) == []
    exp = tibble(x=[1, 2])
    assert out.equals(exp)


def test_0col_df_in_results_ignored():
    df1 = tibble(x=[1, 2])
    df2 = df1 >> group_by(f.x) >> summarise(tibble())
    assert df2.equals(df1)

    df2 = df1 >> group_by(f.x) >> summarise(tibble(), y=65)
    df3 = df1 >> mutate(y=65)
    assert df2.equals(df3)

    df2 = tibble(x=[1, 2], y=[3, 4])
    df3 = df2 >> group_by(f.x) >> summarise(tibble())
    assert df3.equals(df1)

    df3 = df2 >> group_by(f.x) >> summarise(tibble(), z=98)
    df4 = df1 >> mutate(z=98)
    assert df3.equals(df4)


def test_peels_off_a_single_layer_of_grouping():
    df = tibble(
        x=rep([1, 2, 3, 4], each=4), y=rep([1, 2], each=8), z=runif(16)
    )
    gf = df >> group_by(f.x, f.y)

    assert group_vars(summarise(gf)) == ["x"]
    assert group_vars(summarise(summarise(gf))) == []


def test_correctly_reconstructs_groups():
    d = (
        tibble(x=[1, 2, 3, 4], g1=rep([1, 2], 2), g2=[1, 2, 3, 4])
        >> group_by(f.g1, f.g2)
        >> summarise(x=f.x + 1)
    )
    # Different from dplyr, original df does not reorder.
    assert group_rows(d) == [[0, 2], [1, 3]]
    # assert group_rows(d) == [[0,1], [2,3]]


def test_modify_grouping_vars():
    df = tibble(a=c(1, 2, 1, 2), b=c(1, 1, 2, 2))
    gf = group_by(df, f.a, f.b)
    out = summarise(gf, a=f.a + 1)
    assert out.a.obj.tolist() == [2, 3, 2, 3]


def test_allows_names():
    res = (
        tibble(x=[1, 2, 3], y=letters[:3])
        >> group_by(f.y)
        >> summarise(a=length(f.x), b=quantile(f.x, 0.5))
    )
    assert res.b.tolist() == [1.0, 2.0, 3.0]


def test_list_output_columns():
    df = tibble(x=range(1, 11), g=rep([1, 2], each=5))
    res = (
        df
        >> group_by(f.g)
        >> summarise(y=f.x.apply(list))
    )
    assert_iterable_equal(res.y[0], [1, 2, 3, 4, 5])


def test_unnamed_tibbles_are_unpacked():
    df = tibble(x=[1, 2])

    @register_func(context=Context.EVAL)
    def tibble_func(**kwargs):
        return tibble(**kwargs)

    out = summarise(df, tibble_func(y=f.x * 2, z=3))
    assert out.y.tolist() == [2, 4]
    assert out.z.tolist() == [3, 3]


def test_named_tibbles_are_packed():
    @register_func(context=Context.EVAL)
    def tibble_func(**kwargs):
        return tibble(**kwargs)

    df = tibble(x=[1, 2])
    # out = summarise(df, df=tibble_func(y=f.x * 2, z=3)) >> pull(f.df)
    # assert out.y.tolist() == [2, 4]
    # assert out.z.tolist() == [3, 3]


def test_groups_arg(caplog):
    df = tibble(x=1, y=2)
    out = df >> group_by(f.x, f.y) >> summarise()
    assert out.equals(df)
    assert "has grouped output by ['x']" in caplog.text
    caplog.clear()

    out = df >> rowwise(f.x, f.y) >> summarise()
    # assert "[Groups: x, y (n=1)]" in out.attrs["_str_footer"]

    df = tibble(x=1, y=2)
    df1 = df >> summarise(z=3, _groups="rowwise")
    df2 = rowwise(tibble(z=3))
    assert isinstance(df1, TibbleRowwise)
    assert isinstance(df2, TibbleRowwise)
    assert df1.equals(df2)
    assert group_vars(df1) == group_vars(df2)

    gf = df >> group_by(f.x, f.y)
    gvars = gf >> summarise() >> group_vars()
    assert gvars == ["x"]
    gvars = gf >> summarise(_groups="drop_last") >> group_vars()
    assert gvars == ["x"]
    gvars = gf >> summarise(_groups="drop") >> group_vars()
    assert gvars == []
    gvars = gf >> summarise(_groups="keep") >> group_vars()
    assert gvars == ["x", "y"]
    gvars = gf >> summarise(_groups="rowwise") >> group_vars()
    assert gvars == ["x", "y"]
    # assert gvars == []

    rf = df >> rowwise(f.x, f.y)
    gvars = rf >> summarise(_groups="drop") >> group_vars()
    assert gvars == []
    gvars = rf >> summarise(_groups="keep") >> group_vars()
    assert gvars == ["x", "y"]


def test_casts_data_frame_results_to_common_type():
    df = tibble(x=[1, 2], g=[1, 2]) >> group_by(f.g)

    @register_func(context=Context.EVAL)
    def df_of_g(g):
        return g.apply(
            lambda x: tibble(y=1) if x.tolist() == [1] else tibble(y=1, z=2)
        ).reset_index(level=[None], drop=True)

    res = df >> summarise(df_of_g(f.g), _groups="drop")
    assert_iterable_equal(res.z, [None, 2])


def test_silently_skips_when_all_results_are_null():
    df = tibble(x=[1, 2], g=[1, 2]) >> group_by(f.g)

    df1 = summarise(df, x=None)
    df2 = summarise(df)
    assert df1.equals(df2)


def test_errors(caplog):
    df = tibble(x=1, y=2)
    out = df >> group_by(f.x, f.y) >> summarise()
    assert "`summarise()` has grouped output by ['x']" in caplog.text
    assert out.equals(df)
    caplog.clear()

    out = tibble(x=1, y=2) >> group_by(f.x, f.y) >> summarise(z=[2, 2])
    assert "`summarise()` has grouped output by ['x', 'y']" in caplog.text
    exp = tibble(x=[1, 1], y=[2, 2], z=[2, 2])
    assert out.equals(exp)
    caplog.clear()

    out = df >> rowwise(f.x, f.y) >> summarise()
    assert "`summarise()` has grouped output by ['x', 'y']" in caplog.text
    assert out.equals(df)
    caplog.clear()

    out = df >> rowwise() >> summarise()
    assert "`summarise()` has ungrouped output" in caplog.text
    d = dim(out)
    assert d == (1, 0)
    caplog.clear()

    # unsupported type (but python objects are supported by pandas)
    # not testing for types futher
    # tibble(x=1, y=c(1, 2, 2), z=runif(3)) >> summarise(a=object())

    # incompatible size
    with pytest.raises(ValueError):
        tibble(z=1) >> summarise(x=[1, 2, 3], y=[1, 2])
    with pytest.raises(ValueError):
        tibble(z=[1, 2]) >> group_by(f.z) >> summarise(x=[1, 2, 3], y=[1, 2])
    with pytest.raises(ValueError):
        (
            tibble(z=c(1, 3))
            >> group_by(f.z)
            >> summarise(x=seq_len(f.z), y=[1, 2])
        )

    # Missing variable
    with pytest.raises(KeyError):
        summarise(mtcars, a=mean(f.not_there))

    with pytest.raises(KeyError):
        summarise(group_by(mtcars, f.cyl), a=mean(f.not_there))

    # Duplicate column names
    x = 1
    df = tibble(x, x, _name_repair="minimal")
    with pytest.raises(NameNonUniqueError):
        df >> summarise(f.x)


def test_summarise_with_multiple_acrosses():
    """https://stackoverflow.com/questions/63200530/python-pandas-equivalent-to-dplyr-1-0-0-summarizeacross"""
    out = (
        mtcars
        >> group_by(f.cyl)
        >> summarize(across(ends_with("p"), sum), across(ends_with("t"), mean))
    )

    exp = tibble(
        cyl=[6, 4, 8],
        disp=[1283.2, 1156.5, 4943.4],
        hp=[856, 909, 2929],
        drat=[3.585714, 4.070909, 3.229286],
        wt=[3.117143, 2.285727, 3.999214],
    )
    assert_tibble_equal(out, exp)


def test_dup_keyword_args():
    df = tibble(g=[1, 1], a=[1.0, 2.0]) >> group_by(f.g)
    out = df >> summarise(_b=mean(f.a), b=f._b * 2)
    assert_tibble_equal(out, tibble(g=1, b=3.0))


def test_use_pandas_series_func_gh14():
    df = tibble(g=[1, 1, 2, 2], a=[4, 4, 8, 8]) >> group_by(f.g)
    out = df >> summarise(a=f.a.mean())
    # out.a is float64 with pandas 1.3
    # out.a is int64 with pandas 1.2
    # out = out >> mutate(a=as_double(f.a))
    # assert_tibble_equal(out, tibble(g=[1, 2], a=[4.0, 8.0]))


def test_summarise_rowwise():
    params = tibble(sim=[1, 2, 3], n=[1, 2, 3], mean=[1, 2, 1], sd=[1, 4, 2])

    out = params >> rowwise(f.sim) >> summarise(z=rnorm(f.n, f.mean, f.sd))
    assert len(out.columns) == 2
    assert len(out.z.obj.values[0]) == 1
    assert len(out.z.obj.values[1]) == 2
    assert len(out.z.obj.values[2]) == 3
