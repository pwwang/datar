from contextlib import contextmanager

from pipda import register_func


@contextmanager
def _array_ufunc_with_backend(backend: str):
    """Use a backend for the operator"""
    old_backend = array_ufunc.backend
    array_ufunc.backend = backend
    yield
    array_ufunc.backend = old_backend


@register_func(cls=object, dispatchable="first")
def array_ufunc(x, ufunc, *args, **kwargs):
    """Implement the array ufunc

    Allow other backends to override the behavior of the ufunc on
    different types of data.
    """
    return ufunc(x, *args, **kwargs)


array_ufunc.backend = None
array_ufunc.with_backend = _array_ufunc_with_backend
