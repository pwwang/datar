# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-summarise.r
import pytest
from datar.all import *

def test_freshly_create_vars():
    df = tibble(x=range(1,11))
    out = summarise(df, y=mean(f.x), z=f.y+1)
    assert out.y.to_list() == [5.5]
    assert out.z.to_list() == [6.5]

# def test_input_recycled():
#     df1 = tibble() >> summarise(x=1, y=[1,2,3], z=1)
#     df2 = tibble(x=1, y=[1,2,3], z=1)
#     assert df1.equals(df2)

#     gf = group_by(tibble(a = [1,2]), f.a)
#     df1 = gf >> summarise(x=1, y=[1,2,3], z=1)
#     df2 = tibble(
#         a = rep([1,2], each = 3),
#         x = 1,
#         y = c([1,2,3], [1,2,3]),
#         z = 1
#     ) >> group_by(f.a)
#     assert df1.obj.equals(df2.obj)

def test_groups_arg(caplog):
    df = tibble(x=1, y=2)
    out = df >> group_by(f.x, f.y) >> summarise()
    assert out.obj.equals(df)
    assert "regrouping output by ['x']" in caplog.text
    caplog.clear()

    out = df >> rowwise(f.x, f.y) >> summarise()
    # todo after rowwise tests
