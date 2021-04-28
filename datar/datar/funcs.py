"""Basic functions"""
import operator

from pipda import register_func
from ..core.contexts import Context

@register_func(None, context=Context.EVAL)
def itemgetter(*args, **kwargs):
    """Itemgetter as a function for verb"""
    return operator.itemgetter(*args, **kwargs)
