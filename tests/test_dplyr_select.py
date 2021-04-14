# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-select.r
from datar.stats.verbs import setNames
from pandas.core.frame import DataFrame
import pytest
from datar.all import *
from datar.datasets import mtcars

def test_preserves_grouping():
    gf = group_by(tibble(g = [1,2,3], x = [3,2,1]), f.g)

    out = select(gf, h = f.g)
    assert group_vars(out), ["h"]

def test_grouping_variables_preserved_with_a_message(caplog):
    df = tibble(g = [1,2,3], x = [3,2,1]) >> group_by(f.g)
    res = select(df, f.x)
    assert 'Adding missing grouping variables' in caplog.text
    assert res.columns.tolist() == ['g', 'x']

def test_non_syntactic_grouping_variable_is_preserved():
    df = DataFrame({"a b": [1]}) >> group_by("a b") >> select()
    assert df.columns.tolist() == ["a b"]
    df = DataFrame({"a b": [1]}) >> group_by(f["a b"]) >> select()
    assert df.columns.tolist() == ["a b"]

def test_select_doesnot_fail_if_some_names_missing():
    df1 = tibble(x=range(1,11), y=range(1,11), z=range(1,11))
    df2 = setNames(df1, ["x", "y", ""])

    out1 = select(df1, f.x)
    assert out1.equals(tibble(x=range(1,11)))
    out2 = select(df2, f.x)
    assert out2.equals(tibble(x=range(1,11)))

# Special cases -------------------------------------------------
def test_with_no_args_returns_nothing():
    empty = select(mtcars)
    assert ncol(empty) == 0
    assert nrow(empty) == 32

    empty = select(mtcars, **{})
    assert ncol(empty) == 0
    assert nrow(empty) == 32

def test_excluding_all_vars_returns_nothing():
    out = select(mtcars, ~f[f.mpg:f.carb])
    assert dim(out) == (32, 0)

    out = mtcars >> select(starts_with("x"))
    assert dim(out) == (32, 0)

    out = mtcars >> select(~matches("."))
    assert dim(out) == (32, 0)

def test_negating_empty_match_returns_everything():
    df = tibble(x=[1,2,3], y=[3,2,1])
    out = df >> select(~starts_with("xyz"))
    assert out.equals(df)

def test_can_select_with_duplicate_columns():

    df = tibble(tibble(x=1), x=2, y=1, _name_repair="minimal")
    out = select(df, 0, 2)
    assert out.columns.tolist() == ['x', 'y']
    out = select(df, 2, 0)
    assert out.columns.tolist() == ['y', 'x']

    out = select(df, f.y)
    assert out.columns.tolist() == ['y']

# Select variables -----------------------------------------------
def test_can_be_before_group_by():
    df = tibble(
        id = c(1, 1, 2, 2, 2, 3, 3, 4, 4, 5),
        year = c(2013, 2013, 2012, 2013, 2013, 2013, 2012, 2012, 2013, 2013),
        var1 = rnorm(10)
    )
    dfagg = df >> group_by(
        f.id, f.year
    ) >> select(
        f.id, f.year, f.var1
    ) >> summarise(var1 = mean(f.var1))

    assert names(dfagg) == ["id", "year", "var1"]

def test_arguments_to_select_dont_match_vars_select_arguments():
    df = tibble(a=1)
    out = select(df, var=f.a)
    assert out.equals(tibble(var=1))

    out = select(group_by(df, f.a), var=f.a)
    exp = group_by(tibble(var=1), f.var)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

    out = select(df, exclude=f.a)
    assert out.equals(tibble(exclude=1))
    out = select(df, include=f.a)
    assert out.equals(tibble(include=1))

    out = select(group_by(df, f.a), exclude=f.a)
    exp = group_by(tibble(exclude=1), f.exclude)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

    out = select(group_by(df, f.a), include=f.a)
    exp = group_by(tibble(include=1), f.include)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

def test_can_select_data_pronoun():
    out = select(mtcars, mtcars.cyl)
    exp = select(mtcars, f.cyl)
    assert out.equals(exp)

def test_can_select_with_list_of_strs():
    out = select(mtcars, "cyl", "disp", c("cyl", "am", "drat"))
    exp = mtcars[c("cyl", "disp", "am", "drat")]
    assert out.equals(exp)

def test_treats_null_inputs_as_empty():
    out = select(mtcars, NULL, f.cyl, NULL)
    exp = select(mtcars, f.cyl)
    assert out.equals(exp)

def test_can_select_with_strings():
    variabls = dict(foo="cyl", bar="am")
    out = select(mtcars, **variabls)
    exp = select(mtcars, foo=f.cyl, bar=f.am)
    assert out.equals(exp)

def test_works_on_empty_names():
    df = tibble(x=1, y=2, z=3) >> setNames(c("x", "y", ""))
    out = select(df, f.x) >> pull(to='list')
    assert out == [1]

    df >>= setNames(c("", "y", "z"))
    out = select(df, f.y) >> pull(to='list')
    assert out == [2]

def test_works_on_na_names():
    df = tibble(x=1, y=2, z=3) >> setNames(c("x", "y", NA))
    out = select(df, f.x) >> pull(to='list')
    assert out == [1]

    df >>= setNames(c(NA, "y", "z"))
    out = select(df, f.y) >> pull(to='list')
    assert out == [2]

def test_keeps_attributes():
    df = tibble(x=1)
    df.attrs['a'] = 'b'
    out = select(df, f.x)
    assert out.attrs['a'] == 'b'

