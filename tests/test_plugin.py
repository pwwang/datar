import pytest
import numpy as np

from simplug import MultipleImplsForSingleResultHookWarning
from pipda import Context
from datar import f, options_context
from datar.core.plugin import plugin
from datar.core.operator import DatarOperator


class TestPlugin1:

    @plugin.impl
    def array_ufunc(ufunc, x, *args, **kwargs):
        print(ufunc, x, args, kwargs)
        return ufunc(x * 100, *args, **kwargs)

    @plugin.impl
    def get_versions():
        return {"abc": "1.2.3"}

    @plugin.impl
    def load_dataset(name, metadata):
        return name * 2

    @plugin.impl
    def other_api():
        return {"other_var": 1}

    @plugin.impl
    def operate(op, x, y=None):
        if op == "add":
            return x + y + x * y
        return None

    @plugin.impl
    def c_getitem(item):
        return item * 2


class TestPlugin2:

    @plugin.impl
    def array_ufunc(ufunc, x, *args, **kwargs):
        return ufunc(x * 4, *args, **kwargs)

    @plugin.impl
    def load_dataset(name, metadata):
        return name * 3

    @plugin.impl
    def c_getitem(item):
        return item * 4

    @plugin.impl
    def operate(op, x, y=None):
        if op == "add":
            return x + y + 2 * x * y
        return None


def setup():
    plugin.register(TestPlugin1)
    plugin.register(TestPlugin2)
    plugin.get_plugin("testplugin1").disable()
    plugin.get_plugin("testplugin2").disable()


@pytest.fixture
def with_test_plugin1():
    plugin.get_plugin("testplugin1").enable()
    yield
    plugin.get_plugin("testplugin1").disable()


@pytest.fixture
def with_test_plugin2():
    plugin.get_plugin("testplugin2").enable()
    yield
    plugin.get_plugin("testplugin2").disable()


def test_get_versions(with_test_plugin1, capsys):
    from datar import get_versions
    assert get_versions(prnt=False)["abc"] == "1.2.3"

    get_versions()
    assert "datar" in capsys.readouterr().out


def test_other_api(with_test_plugin1):
    from datar import all, other
    plugin.hooks.other_api()
    from importlib import reload
    reload(other)
    assert other.other_var == 1

    reload(all)
    from datar.all import other_var
    assert other_var == 1


def test_load_dataset(with_test_plugin1, with_test_plugin2):
    with pytest.warns(MultipleImplsForSingleResultHookWarning):
        from datar.data import iris

    assert iris == "irisirisiris"

    from datar.data import load_dataset
    assert load_dataset("iris", __backend="testplugin1") == "irisiris"


def test_operate(with_test_plugin1):

    expr = f[0] + f[1]
    assert expr._pipda_eval([3, 2], Context.EVAL) == 11


def test_operate2(with_test_plugin1, with_test_plugin2):
    expr = f[0] + f[1]
    with pytest.warns(MultipleImplsForSingleResultHookWarning):
        assert expr._pipda_eval([3, 2], Context.EVAL) == 17

    with DatarOperator.with_backend("testplugin1"):
        assert expr._pipda_eval([3, 2], Context.EVAL) == 11

    with pytest.warns(MultipleImplsForSingleResultHookWarning):
        assert expr._pipda_eval([3, 2], Context.EVAL) == 17


def test_c_getitem(with_test_plugin1):
    from datar.base import c
    assert c[11] == 22


def test_c_getitem2(with_test_plugin1, with_test_plugin2):
    from datar.base import c
    with pytest.warns(MultipleImplsForSingleResultHookWarning):
        assert c[11] == 44

    with c.with_backend("testplugin1"):
        assert c[11] == 22

    with pytest.warns(MultipleImplsForSingleResultHookWarning):
        assert c[11] == 44


def test_array_ufunc(with_test_plugin1):
    assert np.sqrt(f)._pipda_eval(4) == 20.0


def test_array_ufunc_backend(with_test_plugin1, with_test_plugin2):
    with pytest.warns(MultipleImplsForSingleResultHookWarning):
        assert np.sqrt(f)._pipda_eval(4) == 4.0

    with options_context(ufunc_backend_default="testplugin1"):
        assert np.sqrt(f)._pipda_eval(4) == 20.0

    with options_context(ufunc_backend_default="testplugin2"):
        assert np.sqrt(f)._pipda_eval(4) == 4.0

    with options_context(ufunc_backend={"maximum.accumulate": "testplugin2"}):
        assert np.maximum.accumulate(f)._pipda_eval([1, 2]).tolist() == [
            1, 2, 2, 2, 2, 2, 2, 2
        ]
