# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-pivot-wide.R
import pytest
from datar.all import *
from datar.core.backends.pandas.testing import assert_frame_equal
from ..conftest import assert_iterable_equal, assert_equal

def test_can_pivot_all_cols_to_wide():
    df = tibble(key=list('xyz'), val=c[1:4])
    pv = pivot_wider(df, names_from=f.key, values_from=f.val)
    assert pv.columns.tolist() == list('xyz')
    assert_equal(nrow(pv), 1)

def test_non_pivoted_cols_are_preserved():
    df = tibble(a=1, key=list('xy'), val=c[1:2])
    pv = pivot_wider(df, names_from=f.key, values_from=f.val)

    assert pv.columns.tolist() == list('axy')
    assert_equal(nrow(pv), 1)

def test_implicit_missings_turn_into_explicit_missings():
    df = tibble(a=[1,2], key=['x', 'y'], val=f.a)
    pv = pivot_wider(df, names_from = f.key, values_from = f.val)

    assert_iterable_equal(pv.a, [1,2])
    assert_iterable_equal(pv.x, [1,NA])
    assert_iterable_equal(pv.y, [NA,2])

def test_error_when_overwriting_existing_column():
    df = tibble(
        a=[1,1],
        key=['a', 'b'],
        val=[1,2]
    )
    with pytest.raises(ValueError, match="already exists"):
        pivot_wider(df, names_from=f.key, values_from=f.val)

def test_grouping_is_preserved():
    df = tibble(g=1, k="x", v=2)
    out = df >> group_by(f.g) >> pivot_wider(names_from=f.k, values_from=f.v)
    assert_equal(group_vars(out), ['g'])

def test_double_underscore_j_can_be_used_as_names_from():
    df = tibble(__8=list('xyz'), val=c[1:4], _name_repair='minimal')
    pv = pivot_wider(df, names_from=f.__8, values_from=f.val)

    assert pv.columns.tolist() == ['x', 'y', 'z']
    assert_equal(nrow(pv), 1)

def test_nested_df_pivot_correctly():
    df = tibble(
        i=[1,2,1,2],
        g=list('aabb'),
        d=tibble(x=c[1:5], y=c[5:9])
    )
    out = pivot_wider(df, names_from=f.g, values_from=f.d)
    assert_iterable_equal(out['a$x'], [1,2])
    assert_iterable_equal(out['b$y'], [7,8])

    with pytest.raises(KeyError):
        pivot_wider(df, names_from=f.g, values_from=f.e)

def test_works_with_empty_key_vars():
    df = tibble(n="a", v=1)
    pw = pivot_wider(df, names_from=f.n, values_from=f.v)
    assert_frame_equal(pw, tibble(a=1))

# column names -------------------------------------------------------------

def test_names_glue_affects_output_names():
    df = tibble(x=['X', 'Y'], y=c[1:3], a=f.y, b=f.y)
    out = pivot_wider(
        df,
        names_from=[f.x, f.y],
        values_from=[f.a, f.b],
        names_glue='{x}{y}_{_value}'
    )
    assert out.columns.tolist() == ['X1_a', 'Y2_a', 'X1_b', 'Y2_b']

    # single values_from
    # https://stackoverflow.com/questions/42516817/reproduce-rs-summarise-reshape-result-in-python
    df = tribble(
        f.project, f.resourcetype, f.count,
        1000001,   "O",            7,
        1000002,   "O",            6,
        1000003,   "O",            18,
        1000004,   "C",            1,
        1000004,   "I",            1,
        1000004,   "O",            19,
        1000005,   "I",            2,
        1000005,   "O",            11,
        1000006,   "O",            4,
    )
    out = df >> pivot_wider(
        names_from=f.resourcetype,
        names_glue="count_{resourcetype}",
        values_from=f.count,
    )
    assert_iterable_equal(out.columns, ['project', 'count_C', 'count_I', 'count_O'])

def test_can_sort_column_names():
    df = tibble(
        int=[1,3,2],
        fac=factor(list('abc'),
            levels=list('acb')
        )
    )
    out = pivot_wider(df, names_from=f.fac, values_from=f.int, names_sort=False)
    assert out.columns.tolist() == list('acb')
    out = pivot_wider(df, names_from=f.fac, values_from=f.int, names_sort=True)
    assert out.columns.tolist() == list('abc')


# keys ---------------------------------------------------------

def test_can_override_default_keys():
    df = tribble(
        f.row, f.name, f.var, f.value,
        1,    "Sam", "age", 10,
        2,    "Sam", "height", 1.5,
        3,    "Bob", "age", 20,
    )
    pv = df >> pivot_wider(id_cols = f.name, names_from = f.var, values_from = f.value)
    assert_equal(nrow(pv), 2)


# non-unqiue keys ---------------------------------------------------------

# instead of list-columns
def test_duplicated_keys_aggregated_by_values_fn():
    df = tibble(a = c(1, 1, 2), key = c("x", "x", "x"), val = c[1:4])
    pv = pivot_wider(df, names_from = f.key, values_from = f.val, values_fn=mean) # mean by default
    assert_iterable_equal(pv.x, [1.5, 3.0])
    pv = pivot_wider(df, names_from = f.key, values_from = f.val, values_fn=sum)
    assert_iterable_equal(pv.x, [3.0, 3.0])

def test_duplicated_keys_produce_list_column_with_error():
    df = tibble(a = c(1, 1, 2), key = c("x", "x", "x"), val = c[1:4])
    with pytest.raises(ValueError, match="aggregated value"):
        pivot_wider(df, names_from = f.key, values_from = f.val)

def test_values_fn_can_keep_list():
    df = tibble(a = c(1, 1, 2), key = c("x", "x", "x"), val = c[1:4])
    pv = pivot_wider(df, names_from = f.key, values_from = f.val, values_fn=list)
    assert_iterable_equal(pv.a, [1,2])
    assert pv.x.tolist() == [[1,2], [3]]

def test_values_fn_to_be_a_single_func():
    df = tibble(a = c(1, 1, 2), key = c("x", "x", "x"), val = c(1, 10, 100))
    pv = pivot_wider(df, names_from=f.key, values_from=f.val, values_fn=sum)
    assert_iterable_equal(pv.x, [11,100])

# test_that("values_summarize applied even when no-duplicates", {
#   df <- tibble(a = c(1, 2), key = c("x", "x"), val = 1:2)
#   pv <- pivot_wider(df,
#     names_from = key,
#     values_from = val,
#     values_fn = list(val = list)
#   )

#   expect_equal(pv$a, c(1, 2))
#   expect_equal(as.list(pv$x), list(1L, 2L))
# })


# can fill missing cells --------------------------------------------------

def test_can_fill_in_missing_cells():
    df = tibble(g = c(1, 2), var = c("x", "y"), val = c(1, 2))
    widen = lambda **kwargs: df >> pivot_wider(names_from=f.var, values_from=f.val, **kwargs)

    assert_iterable_equal(widen().x, [1, NA])
    assert_iterable_equal(widen(values_fill=0).x, [1,0])
    # assert_iterable_equal(widen(values_fill={'val': 0}).x, [1,0])

def test_values_fill_only_affects_missing_cells():
    df = tibble(g = c(1, 2), names = c("x", "y"), value = c(1, NA))
    out = pivot_wider(df, names_from=f.names, values_from=f.value, values_fill=0)
    assert_iterable_equal(out.y, [0, NA])

# multiple values ----------------------------------------------------------

def test_can_pivot_from_multiple_measure_cols():
    df = tibble(row = 1, var = c("x", "y"), a = c[1:3], b = c[3:5])
    sp = pivot_wider(df, names_from=f.var, values_from=[f.a, f.b])
    assert sp.columns.tolist() == ['row', 'a_x', 'a_y', 'b_x', 'b_y']
    assert_iterable_equal(sp.a_x, [1])
    assert_iterable_equal(sp.b_y, [4])

def test_can_pivot_from_multiple_measure_cols_using_all_keys():
    df = tibble(var = c("x", "y"), a = c[1:3], b = c[3:5])
    sp = pivot_wider(df, names_from=f.var, values_from=[f.a, f.b])
    assert sp.columns.tolist() == ['a_x', 'a_y', 'b_x', 'b_y']
    assert_iterable_equal(sp.a_x, [1])
    assert_iterable_equal(sp.b_y, [4])

# test_that("column order in output matches spec", {
#   df <- tribble(
#     ~hw,   ~name,  ~mark,   ~pr,
#     "hw1", "anna",    95,  "ok",
#     "hw2", "anna",    70, "meh",
#   )

#   # deliberately create weird order
#   sp <- tribble(
#     ~hw, ~.value,  ~.name,
#     "hw1", "mark", "hw1_mark",
#     "hw1", "pr",   "hw1_pr",
#     "hw2", "pr",   "hw2_pr",
#     "hw2", "mark", "hw2_mark",
#   )

#   pv <- pivot_wider_spec(df, sp)
#   expect_named(pv, c("name", sp$.name))
# })
