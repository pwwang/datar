from pipda import register_array_ufunc

from .options import get_option
from .plugin import plugin


def _array_ufunc_to_register(ufunc, x, *args, **kwargs):
    """Register the array ufunc to pipda"""
    from ..apis.misc import array_ufunc

    return array_ufunc(
        x,
        ufunc,
        *args,
        **kwargs,
        __backend=array_ufunc.backend,
    )


plugin.load_entrypoints(only=get_option("backends"))

plugin.hooks.setup()
register_array_ufunc(_array_ufunc_to_register)
