"""String functions in R"""
from typing import Any

import numpy
from pandas.core.dtypes.common import is_string_dtype
from pipda import register_func

from ..core.contexts import Context
from ..core.types import StringOrIter

from .casting import _as_type
from .testing import _register_type_testing

# grep, grepl, sub, gsub, regexpr, gregexpr, regexec, gregexec
# agrep, agrepl
# nchar, nzchar
# paste, paste0
# sprintf, gettextf
# substr, substring
# strsplit
# abbreviate
# format, format_info, format_pval, format_c, pretty_num (_format_zeros?)
#

# pylint: disable=invalid-name

@register_func(None, context=Context.EVAL)
def as_character(x: Any) -> StringOrIter:
    """Convert an object or elements of an iterable into string

    Aliases `as_str` and `as_string`

    Args:
        x: The object

    Returns:
        When x is an array or a series, return x.astype(str).
        When x is iterable, convert elements of it into strings
        Otherwise, convert x to string.
    """
    return _as_type(x, str)

as_str = as_string = as_character

is_character = _register_type_testing(
    'is_character',
    scalar_types=(str, numpy.str_),
    dtype_checker=is_string_dtype,
    doc="""Test if a value is characters/string

Alias `is_str` and `is_string`

Args:
    x: The value to be checked

Returns:
    True if the value is string; with a string dtype;
    or all elements are strings
"""
)

is_str = is_string = is_character
