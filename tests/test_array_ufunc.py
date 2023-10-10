import pytest  # noqa: F401

import numpy as np
from pipda import Context
from datar import f
from datar.core import plugin as _  # noqa: F401
from datar.apis.misc import array_ufunc


def test_default():
    out = np.sqrt(f)._pipda_eval([1, 4, 9], Context.EVAL)
    assert out.tolist() == [1, 2, 3]


def test_misc_obj():
    class Foo(list):
        pass

    @array_ufunc.register(Foo)
    def _array_ufunc(x, ufunc, *args, kind, **kwargs):
        return ufunc([i * 2 for i in x], *args, **kwargs)

    out = np.sqrt(f)._pipda_eval(Foo([2, 8, 18]), Context.EVAL)
    assert out.tolist() == [2, 4, 6]
