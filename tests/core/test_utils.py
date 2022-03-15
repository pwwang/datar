import pytest

from pandas.testing import assert_frame_equal
from datar.core.utils import *
from datar.all import factor, levels, NA, tibble, is_integer
from ..conftest import assert_iterable_equal

# def test_na_if_safe():
#     fct = factor([1,2,3])
#     out = na_if_safe(fct, 1)
#     assert_iterable_equal(out, [NA, 2, 3])
#     assert_iterable_equal(levels(out), [2, 3])

#     it = [1,2,3]
#     out = na_if_safe(it, 1)
#     assert_iterable_equal(out, [NA, 2, 3])

# def test_fillna_safe():
#     it = [1,2,NA]
#     with pytest.raises(ValueError):
#         fillna_safe(it, 1)

#     it = Categorical(it, categories=[1,2])
#     out = fillna_safe(it, 3)
#     assert_iterable_equal(out, [1,2,3])
#     assert_iterable_equal(out.categories, [1,2,3])

# def test_df_setitem():
#     df = tibble(x=1)
#     out = df_setitem(df, 'y', numpy.array([[1,2]], dtype=object))
#     assert_frame_equal(out, tibble(x=1, y=[[1,2]]))
#     out = df_setitem(df, 'z', (1,))
#     assert_frame_equal(out, tibble(x=1, y=[[1,2]], z=1))

# def test_keep_column_order():
#     df = tibble(x=1, y=2)
#     with pytest.raises(ValueError):
#         keep_column_order(df, ['x'])

def test_apply_dtypes():
    df = tibble(x=[1.0, 2.0])
    apply_dtypes(df, True)
    assert is_integer(df.x)

def test_arg_match():
    with pytest.raises(ValueError, match='abc'):
        arg_match('a', 'a', ['b', 'c'], errmsg='abc')
    with pytest.raises(ValueError, match='must be one of'):
        arg_match('a', 'a', ['b', 'c'])

# def test_name_mutatable_args():
#     out = name_mutatable_args(tibble(a=1), a={'x':1})
#     assert out == {'a': {'x': 1}}

# def test_dict_insert_at():
#     dic = dict(a=1, b=2, c=3)
#     out = dict_insert_at(dic, 'b', {'d': 4})
#     assert out == dict(a=1, d=4, b=2, c=3)
#     out = dict_insert_at(dic, 'b', {'d': 4}, remove=True)
#     assert out == dict(a=1, d=4, c=3)
#     dic = dict(a=1, b=2, c=3)
#     out = dict_insert_at(dic, ['b', 'c'], {'b': 4}, remove=False)
#     assert out == dict(a=1, b=4, c=3)

# def test_to_df():
#     df = tibble(x=1, y=2)
#     assert_frame_equal(to_df(df), df)

#     out = to_df(df, name='a')
#     assert_iterable_equal(out.columns, ['a$x', 'a$y'])

#     d = numpy.array([[1,2,3]])
#     out = to_df(d, ['x', 'y', 'z'])
#     assert_frame_equal(out, tibble(x=1, y=2, z=3))

#     out = to_df(d, ['x'])
#     assert_iterable_equal(out.columns, [0,1,2])

#     d = numpy.array([[1], [2], [3]])
#     out = to_df(d, 'x')
#     assert_frame_equal(out, tibble(x=[1,2,3]))

#     d = {'x': [1]}
#     assert_frame_equal(to_df(d), tibble(x=1))

# def test_recycle_value():
#     df = tibble(x=[])
#     out = recycle_value(df, 1)
#     assert_frame_equal(out, tibble(x=NA))


def test_dict_get():
    d = {'a': 1, 'b': 2, np.nan: 3}
    assert dict_get(d, 'a') == 1
    assert dict_get(d, 'b') == 2
    assert dict_get(d, float("nan")) == 3
    assert dict_get(d, 'c', None) is None
    with pytest.raises(KeyError):
        dict_get(d, 'c')
