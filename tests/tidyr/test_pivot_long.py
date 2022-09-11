# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-pivot-long.R
import pytest
from datar.all import *

from datar.core.backends.pandas.testing import assert_frame_equal
from ..conftest import assert_iterable_equal

def test_can_pivot_all_cols_to_long():
    df = tibble(x=c[1:3], y=c[3:5])
    pv = pivot_longer(df, f[f.x:])

    assert pv.columns.tolist() == ['name', 'value']
    # assert_iterable_equal(pv.name, rep(df.columns, 2))
    assert_iterable_equal(pv.name, rep(df.columns, each=2))
    # assert_iterable_equal(pv.value, [1,3,2,4])
    assert_iterable_equal(pv.value, [1,2,3,4])

    pv2 = pivot_longer(df, f[f.x:], names_transform=str.upper)
    assert_iterable_equal(pv2.name, ['X', 'X', 'Y', 'Y'])
    pv3 = pivot_longer(df, f[f.x:], names_transform={'name': str.upper})
    assert_iterable_equal(pv3.name, ['X', 'X', 'Y', 'Y'])

def test_values_interleaved_correctly():
    df = tibble(
        x=[1,2],
        y=[10,20],
        z=[100,200]
    )
    pv = pivot_longer(df, c[:4])
    # assert_iterable_equal(pv.value, [1,10,100,2,20,200])
    assert_iterable_equal(pv.value, [1,2, 10,20, 100,200])

# test_that("can add multiple columns from spec", {
#   df <- tibble(x = 1:2, y = 3:4)
#   sp <- tibble(.name = c("x", "y"), .value = "v", a = 1, b = 2)
#   pv <- pivot_longer_spec(df, spec = sp)

#   expect_named(pv, c("a", "b", "v"))
# })

def test_preserves_original_keys():
    df = tibble(x=c[1:3], y=2, z=c[1:3])
    pv = pivot_longer(df, f[f.y:])

    assert pv.columns.tolist() == ['x', 'name', 'value']
    assert_iterable_equal(pv.x, rep(df.x, 2))

def test_can_drop_missing_values():
    df = tibble(x=c(1,NA), y=c(NA,2))
    pv = pivot_longer(df, f[f.x:], values_drop_na=True)

    assert_iterable_equal(pv.name, ['x', 'y'])
    assert_iterable_equal(pv.value, [1,2])

def test_can_handle_missing_combinations():
    df = tribble(
        f.id, f.x_1, f.x_2, f.y_2,
        "A",  1,     2,     "a",
        "B",  3,     4,     "b",
    )
    pv = pivot_longer(df, ~f.id, names_to = c(".value", "n"), names_sep = "_")

    assert_iterable_equal(pv.columns, ['id', 'n', 'x', 'y'])
    assert_iterable_equal(pv.x, [1,2,3,4])
    assert_iterable_equal(pv.y, [NA, "a", NA, "b"])

    df = tribble(
        f.id, f.x_1, f.x_2, f.y_2,
        "A",  1,     2,     "a",
        "A",  3,     4,     "b",
    )
    pv = pivot_longer(df, ~f.id, names_to = c(".value", "n"), names_sep = "_")

    assert_iterable_equal(pv.columns, ['id', 'n', 'x', 'y'])
    assert_iterable_equal(pv.x, [1,2,3,4])
    assert_iterable_equal(pv.y, [NA, "a", NA, "b"])

def test_mixed_columns_are_automatically_coerced():
    df = tibble(x = factor("a"), y = factor("b"))
    pv = pivot_longer(df, f[f.x:])
    assert is_factor(pv.value)
    assert_iterable_equal(pv.value, ['a', 'b'])

def test_can_override_default_output_column_type():
    df = tibble(x="x", y=1)
    pv = pivot_longer(df, f[f.x:], values_transform={'value': lambda x: [x]})
    assert pv.value.tolist() == [['x'], [1]]
    pv2 = pivot_longer(df, f[f.x:], values_transform=lambda x: [x])
    assert pv2.value.tolist() == [['x'], [1]]

# test_that("can pivot to multiple measure cols", {
#   df <- tibble(x = "x", y = 1)
#   sp <- tribble(
#     ~.name, ~.value, ~row,
#     "x", "X", 1,
#     "y", "Y", 1,
#   )
#   pv <- pivot_longer_spec(df, sp)

#   expect_named(pv, c("row", "X", "Y"))
#   expect_equal(pv$X, "x")
#   expect_equal(pv$Y, 1)
# })

def test_original_col_order_is_preserved():
    df = tribble(
        f.id, f.z_1, f.y_1, f.x_1, f.z_2, f.y_2, f.x_2,
        "A",  1,     2,    3, 4, 5, 6,
        "B", 7, 8, 9, 10, 11, 12
    )
    pv = pivot_longer(df, ~f.id, names_to = c(".value", "n"), names_sep = "_")
    assert pv.columns.tolist() == ['id', 'n', 'z', 'y', 'x']

def test_handles_duplicated_column_names():
    df = tibble(tibble(a=1), tibble(b=3), x=1, a=2, b=4, _name_repair="minimal")
    pv = pivot_longer(df, ~f.x)

    assert pv.columns.tolist() == ['x', 'name', 'value']
    assert_iterable_equal(pv.name, list('abab'))
    assert_iterable_equal(pv.value, [1,3,2,4])

def test_can_pivot_duplicated_names_to_dot_value():
    df = tibble(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4, _name_repair='minimal')
    pv1 = pivot_longer(df, ~f.x, names_to = c(".value", NA), names_sep = "_")
    pv2 = pivot_longer(df, ~f.x, names_to = c(".value", NA), names_pattern = "(.)_(.)")
    # The suffices will be used to group the data, which needs to be captured explictly.
    # pv3 = pivot_longer(df, ~f.x, names_to = ".value", names_pattern = "(.)_.")

    assert pv1.columns.tolist() == ['x', 'a', 'b']
    assert_iterable_equal(pv1.a, [1,2])
    assert_frame_equal(pv2, pv1)
    # assert_frame_equal(pv3, pv1)

def test_dot_value_can_be_any_position_in_names_to():
    samp = tibble(
        i=c[1:5],
        y_t1=rnorm(4),
        y_t2=rnorm(4),
        z_t1=rep(3,4),
        z_t2=rep(-2,4)
    )
    value_first = pivot_longer(
        samp, ~f.i,
        names_to=['.value', 'time'],
        names_sep="_"
    )

    samp2 = rename(
        samp,
        t1_y='y_t1',
        t2_y='y_t2',
        t1_z='z_t1',
        t2_z='z_t2'
    )
    value_second = pivot_longer(
        samp2, ~f.i,
        names_to = c("time", "_value"),
        names_sep = "_"
    )

    assert_frame_equal(value_first, value_second)

def test_type_error_message_use_variable_names():
    df = tibble(abc=1, xyz="b")
    # no error, dtype falls back to object
    pv = pivot_longer(df, everything())
    assert pv.value.dtype == object

def test_grouping_is_preserved():
    df = tibble(g=1, x1=1, x2=2)
    out = df >> group_by(f.g) >> pivot_longer(
        f[f.x1:],
        names_to="x",
        values_to="v"
    )
    assert group_vars(out) == ['g']

def test_values_to_at_end_of_output():
    df = tibble(
        country=['US', 'CN'],
        new_sp_m014=[159, 22],
        new_sp_m24=[1571,21],
        new_ep_f88=[34, 24]
    )
    pv = pivot_longer(df, ~f.country,
                      names_to=['diagnosis', 'gender', 'age'],
                      names_pattern=r"new_?(.*)_(.)(.*)",
                      values_to = "count")
    assert pv.columns.tolist() == [
        'country', 'diagnosis', 'gender', 'age', 'count'
    ]

def test_errors_names_sep_names_pattern():
    df = tribble(
        f.id, f.x_1, f.x_2, f.y_2,
        "A",  1,     2,     "a",
        "B",  3,     4,     "b",
    )
    with pytest.raises(ValueError):
        pivot_longer(
            df, ~f.id,
            names_to = c(".value", "n"),
            names_sep = "_",
            names_pattern=r'(.)_(.)'
        )
    with pytest.raises(ValueError):
        pivot_longer(
            df, ~f.id,
            names_to = c(".value", "n")
        )

def test_names_prefix():
    df = tribble(
        f.id, f.x_x_1, f.x_x_2, f.x_y_2,
        "A",  1,     2,     "a",
        "B",  3,     4,     "b",
    )
    pv = pivot_longer(df, ~f.id,
                      names_to = c(".value", "n"),
                      names_sep = "_",
                      names_prefix='x_')

    assert_iterable_equal(pv.columns, ['id', 'n', 'x', 'y'])
    assert_iterable_equal(pv.x, [1,2,3,4])
    assert_iterable_equal(pv.y, [NA, "a", NA, "b"])
