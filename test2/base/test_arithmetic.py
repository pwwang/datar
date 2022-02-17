import pytest

from pandas import Series
from pandas.core.groupby import SeriesGroupBy
from pandas.testing import assert_series_equal
from datar2.core.tibble import SeriesRowwise
from datar import f
from datar2.testing import assert_tibble_equal
from datar2.base import NA, colnames
# from datar2.datar import drop_index
from datar2.tibble import tibble, tribble
from datar2.base.arithmetic import *
from datar2.base import pi
from ..conftest import assert_iterable_equal


def test_sum():
    assert sum(1) == 1
    assert sum([1,2]) == 3
    assert_iterable_equal([sum([1,2, NA], na_rm=False)], [NA])
    assert sum([1,2,NA], na_rm=True) == 3
    Series
    assert sum(Series([1,2])) == 3
    # Series GroupBy
    out = sum(Series([1,2,3,4]).groupby([1,1,2,2]))
    assert_series_equal(out, Series([3, 7], index=[1, 2]))

# def test_mean():
#     assert mean(1) == 1
#     assert mean([1.0, 2.0]) == 1.5

# def test_median():
#     assert median(1) == 1
#     assert median([1.0, 20, 3.0]) == 3.0

# def test_min():
#     assert min(1) == 1
#     assert min([1,2,3]) == 1

# def test_max():
#     assert max(1) == 1
#     assert max([1,2,3]) == 3

# def test_var():
#     with pytest.warns(RuntimeWarning):
#         assert_iterable_equal([var(1)], [NA])
#     assert var([1,2,3]) == 1

# def test_pmin():
#     assert_iterable_equal(pmin(1,2,3), [1])
#     assert_iterable_equal(pmin(1,[-1, 2], [0, 3]), [-1, 1])

# def test_pmax():
#     assert_iterable_equal(pmax(1,2,3), [3])
#     assert_iterable_equal(pmax(1,[-1, 2], [0, 3]), [1, 3])

# def test_round():
#     assert round(1.23456) == 1.0
#     assert_iterable_equal(round([1.23456, 3.45678]), [1.0, 3.0])
#     assert_iterable_equal(round([1.23456, 3.45678], 1), [1.2, 3.5])

# def test_sqrt():
#     assert sqrt(1) == 1
#     with pytest.warns(RuntimeWarning):
#         assert_iterable_equal([sqrt(-1)], [NA])


# def test_abs():
#     assert abs(1) == 1
#     assert_iterable_equal(abs([-1, 1]), [1, 1])

# def test_ceiling():
#     assert ceiling(1.1) == 2
#     assert_iterable_equal(ceiling([-1.1, 1.1]), [-1, 2])

# def test_floor():
#     assert floor(1.1) == 1
#     assert_iterable_equal(floor([-1.1, 1.1]), [-2, 1])

# def test_cov():
#     df = tibble(x=f[1:3], y=f[4:6])
#     out = df >> cov() >> drop_index()
#     assert_frame_equal(out, tibble(x=[1.0,1.0], y=[1.0,1.0]))

#     out = [1,2,3] >> cov([4,5,6])
#     assert out == 1.0

# def test_col_row_verbs():
#     df = tribble(
#         f.x, f.y, f.z,
#         1,   NA,  6,
#         2,   4,   9,
#         3,   6,   15
#     )
#     assert_iterable_equal(row_medians(df), [NA, 4, 6])
#     assert_iterable_equal(row_medians(df, na_rm=True), [3.5, 4, 6])
#     assert_iterable_equal(col_medians(df), [2, NA, 9])
#     assert_iterable_equal(col_medians(df, na_rm=True), [2, 5, 9])

#     assert_iterable_equal(row_means(df), [NA, 5, 8])
#     assert_iterable_equal(row_means(df, na_rm=True), [3.5, 5, 8])
#     assert_iterable_equal(col_means(df), [2, NA, 10])
#     assert_iterable_equal(col_means(df, na_rm=True), [2, 5, 10])

#     assert_iterable_equal(row_sums(df), [NA, 15, 24])
#     assert_iterable_equal(row_sums(df, na_rm=True), [7, 15, 24])
#     assert_iterable_equal(col_sums(df), [6, NA, 30])
#     assert_iterable_equal(col_sums(df, na_rm=True), [6, 10, 30])

#     assert_iterable_equal(
#         row_sds(df),
#         [NA, 3.605551275463989, 6.244997998398398],
#         approx=True
#     )
#     assert_iterable_equal(
#         row_sds(df, na_rm=True),
#         [3.5355339059327378, 3.605551275463989, 6.244997998398398],
#         approx=True
#     )
#     assert_iterable_equal(
#         col_sds(df),
#         [1.0, NA, 4.58257569495584],
#         approx=True
#     )
#     assert_iterable_equal(
#         col_sds(df, na_rm=True),
#         [1.0, 1.4142135623730951, 4.58257569495584],
#         approx=True
#     )

# def test_scale():
#     out =  [1,2,3] >> scale()
#     assert_frame_equal(out, tibble(scaled=[-1.,0.,1.]))
#     assert_iterable_equal(out.attrs['scaled:center'], [2])
#     assert_iterable_equal(out.attrs['scaled:scale'], [1])

#     out = scale([1,2,3], center=1)
#     assert_frame_equal(out, tibble(scaled=[0.,0.6324555,1.2649111]))
#     assert_iterable_equal(out.attrs['scaled:center'], [1])
#     assert_iterable_equal(out.attrs['scaled:scale'], [1.581139], approx=True)

#     out = [1,2,3] >> scale(scale=1)
#     assert_frame_equal(out, tibble(scaled=[-1.,0.,1.]))
#     assert_iterable_equal(out.attrs['scaled:center'], [2])
#     assert_iterable_equal(out.attrs['scaled:scale'], [1])

#     with pytest.raises(ValueError):
#         scale([1,2,3], center=[1,2])
#     with pytest.raises(ValueError):
#         [1,2,3] >> scale(scale=[1,2])

#     df = tibble(x=[1,2,3], y=[4,5,6])
#     assert_frame_equal(scale(df, False, False), df)

# def test_signif():
#     x2 = pi * 100. ** Array([-1,0,1,2,3])
#     out = signif(x2, 3)
#     assert_iterable_equal(
#         out,
#         [3.14e-02, 3.14e+00, 3.14e+02, 3.14e+04, 3.14e+06],
#         approx=True
#     )

# def test_log():
#     assert pytest.approx(log(exp(1))) == 1.0
#     assert pytest.approx(log(4, 4)) == 1.0
#     assert pytest.approx(log([exp(1), exp(2)])) == [1.0, 2.0]
#     assert pytest.approx(log2(2)) == 1.0
#     assert pytest.approx(log10(10)) == 1.0
