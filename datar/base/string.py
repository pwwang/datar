"""String functions in R"""
import re
from typing import Any, Iterable, Tuple, Union

import numpy
from pandas.core.dtypes.common import is_string_dtype
from pipda import register_func

from ..core.contexts import Context
from ..core.types import Dtype, IntOrIter, StringOrIter, is_scalar, is_null
from ..core.utils import (
    arg_match,
    get_option,
    length_of,
    logger,
    Array,
    position_at,
    recycle_value,
)

from .casting import _as_type
from .testing import _register_type_testing
from .na import NA

# agrep, agrepl
# format, format_info, format_pval, format_c, pretty_num (_format_zeros?)
#

# pylint: disable=invalid-name


@register_func(None, context=Context.EVAL)
def as_character(x: Any, str_dtype: Dtype = str, _na: Any = NA) -> StringOrIter:
    """Convert an object or elements of an iterable into string

    Aliases `as_str` and `as_string`

    Args:
        x: The object
        str_dtype: The string dtype to convert to
        _na: How NAs should be casted. Specify NA will keep them unchanged.
            But the dtype will be object then.

    Returns:
        When x is an array or a series, return x.astype(str).
        When x is iterable, convert elements of it into strings
        Otherwise, convert x to string.
    """
    return _as_type(x, str_dtype, na=_na)

as_str = as_string = as_character

is_character = _register_type_testing(
    "is_character",
    scalar_types=(str, numpy.str_),
    dtype_checker=is_string_dtype,
    doc="""Test if a value is characters/string

Alias `is_str` and `is_string`

Args:
    x: The value to be checked

Returns:
    True if the value is string; with a string dtype;
    or all elements are strings
""",
)

is_str = is_string = is_character


# Grep family -----------------------------------


@register_func(None, context=Context.EVAL)
def grep(
    pattern: StringOrIter,
    x: StringOrIter,
    ignore_case: bool = False,
    value: bool = False,
    fixed: bool = False,
    invert: bool = False,
    base0_: bool = None,
) -> Iterable[Union[int, str]]:
    """R's grep, get the element in x matching the pattern

    Args:
        pattern: The pattern
        x: A string or an iterable of strings; or those can be coerced to
        ignore_case: Do case-insensitive matching?
        value: Return values instead of indices?
        fixed: Fixed matching (instead of regex matching)?
        invert: Return elements thata don't match instead?
        base0_: When return indices, whether return 0-based indices?
            If not set, will use `datar.base.get_option('which.base.0')`

    Returns:
        The matched (or unmatched (`invert=True`)) indices
        (or values (`value=True`)).
    """
    if is_scalar(x):
        x = [x] # type: ignore
    x = Array(as_character(x), dtype=object)
    matched = grepl(
        pattern=pattern,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
        invert=invert,
    )

    if value:
        return x[matched]

    base0_ = get_option("which.base.0", base0_)
    return numpy.flatnonzero(matched) + int(not base0_)


@register_func(None, context=Context.EVAL)
def grepl(
    pattern: StringOrIter,
    x: StringOrIter,
    ignore_case: bool = False,
    fixed: bool = False,
    invert: bool = False,
) -> Iterable[Union[int, str]]:
    """R's grepl, check whether elements in x matching the pattern

    Args:
        pattern: The pattern
        x: A string or an iterable of strings; or those can be coerced to
        ignore_case: Do case-insensitive matching?
        fixed: Fixed matching (instead of regex matching)?
        invert: Return elements thata don't match instead?

    Returns:
        A bool array indicating whether the elements in x match the pattern
    """
    pattern = _warn_more_pat_or_rep(pattern, "grep")
    match_fun = lambda text: _match(
        pattern, text, ignore_case=ignore_case, invert=invert, fixed=fixed
    )
    if is_scalar(x):
        x = [x] # type: ignore
    x = Array(as_character(x), dtype=object)
    return Array(list(map(match_fun, x)), dtype=bool)


@register_func(None, context=Context.EVAL)
def sub(
    pattern: StringOrIter,
    replacement: StringOrIter,
    x: StringOrIter,
    ignore_case: bool = False,
    fixed: bool = False,
) -> Iterable[str]:
    """R's sub, replace a pattern with replacement for elements in x,
    each only once

    Args:
        pattern: The pattern
        replacement: The replacement
        x: A string or an iterable of strings; or those can be coerced to
        ignore_case: Do case-insensitive matching?
        fixed: Fixed matching (instead of regex matching)?

    Returns:
        An array of strings with matched parts replaced.
    """
    return _sub(
        pattern=pattern,
        replacement=replacement,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
    )


@register_func(None, context=Context.EVAL)
def gsub(
    pattern: StringOrIter,
    replacement: StringOrIter,
    x: StringOrIter,
    ignore_case: bool = False,
    fixed: bool = False,
) -> Iterable[str]:
    """R's gsub, replace a pattern with replacement for elements in x,
    each for all matched parts

    See Also:
        [sub()](datar.base.string.sub)
    """
    return _sub(
        pattern=pattern,
        replacement=replacement,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
        count=0,
        fun="gsub",
    )


# Grep family helpers --------------------------------


def _warn_more_pat_or_rep(
    pattern: StringOrIter, fun: str, arg: str = "pattern"
) -> str:
    """Warn when there are more than one pattern or replacement provided"""
    if is_scalar(pattern):
        return pattern
    if len(pattern) == 1:
        return pattern[0]

    logger.warning(
        "In %s(...), argument `%s` has length > 1 and only the "
        "first element will be used",
        fun,
        arg,
    )
    return pattern[0]


def _match(
    pattern: str, text: str, ignore_case: bool, invert: bool, fixed: bool
) -> bool:
    """Do the regex match"""
    if is_null(text):
        return False

    flags = re.IGNORECASE if ignore_case else 0
    if fixed:
        pattern = re.escape(pattern)

    pattern = re.compile(pattern, flags)
    matched = pattern.search(text)
    if invert:
        matched = not bool(matched)
    return bool(matched)


def _sub(
    pattern: StringOrIter,
    replacement: StringOrIter,
    x: StringOrIter,
    ignore_case: bool = False,
    fixed: bool = False,
    count: int = 1,
    fun: str = "sub",
) -> Iterable[str]:
    """Replace a pattern with replacement for elements in x,
    with argument count available
    """
    pattern = _warn_more_pat_or_rep(pattern, fun)
    replacement = _warn_more_pat_or_rep(replacement, fun, "replacement")
    if fixed:
        pattern = re.escape(pattern)

    flags = re.IGNORECASE if ignore_case else 0
    pattern = re.compile(pattern, flags)

    if is_scalar(x):
        return pattern.sub(repl=replacement, count=count, string=x)

    return Array(
        [pattern.sub(repl=replacement, count=count, string=elem) for elem in x],
        dtype=object,
    )


# nchar family -------------------------------

# pylint: disable=redefined-builtin


@register_func(None, context=Context.EVAL)
def nchar(
    x: StringOrIter,
    type: str = "chars",
    allow_na: bool = True,  # i.e.: '\ud861'
    keep_na: bool = None,
    _na_len: int = 2,
) -> Iterable[int]:
    """Get the size of the elements in x"""
    x, keep_na = _prepare_nchar(x, type, keep_na)
    out = [
        _nchar_scalar(
            elem, retn=type, allow_na=allow_na, keep_na=keep_na, na_len=_na_len
        )
        for elem in x
    ]
    if is_null(out).any():
        return Array(out, dtype=object)
    return Array(out, dtype=int)


@register_func(None, context=Context.EVAL)
def nzchar(x: StringOrIter, keep_na: bool = False) -> Iterable[bool]:
    """Find out if elements of a character vector are non-empty strings.

    Args:
        x: Strings to test
        keep_na: Keep NAs as is?

    Returns:
        A bool array to tell whether elements in x are non-empty strings
    """
    if is_scalar(x):
        x = [x] # type: ignore
    x = as_character(x, _na=NA if keep_na else None)
    out = [NA if is_null(elem) else bool(elem) for elem in x]
    if is_null(out).any():
        return Array(out, dtype=object)
    return Array(out, dtype=bool)


# nchar helpers --------------------------------


def _prepare_nchar(
    x: StringOrIter, type: str, keep_na: bool
) -> Tuple[Iterable[str], bool]:
    """Prepare arguments for n(z)char"""
    arg_match(type, "type", ["chars", "bytes", "width"])
    if keep_na is None:
        keep_na = type != "width"

    if is_scalar(x):
        x = [x] # type: ignore
    x = Array(as_character(x), dtype=object)
    return x, keep_na


def _nchar_scalar(
    x: str, retn: str, allow_na: bool, keep_na: bool, na_len: int
) -> int:
    """Get the size of a scalar string"""
    if is_null(x):
        return NA if keep_na else na_len

    if retn == "width":
        from wcwidth import wcswidth

        return wcswidth(x)
    if retn == "chars":
        return len(x)

    try:
        x = x.encode("utf-8")
    except UnicodeEncodeError:
        if allow_na:
            return NA
        raise
    return len(x)


# paste and paste0 --------------------


@register_func(None, context=Context.EVAL)
def paste(
    *args: StringOrIter, sep: str = " ", collapse: str = None
) -> StringOrIter:
    """Concatenate vectors after converting to character.

    Args:
        *args: strings to be concatenated
        sep: The separator
        collapse: The separator to collapse the final string arrays

    Returns:
        A single string if collapse is given, otherwise an array of strings.
    """
    args = [as_character(arg, _na="NA") for arg in args]
    maxlen = max(map(length_of, args))
    args = zip(*(recycle_value(arg, maxlen) for arg in args))

    out = [sep.join(arg) for arg in args]
    if collapse is not None:
        return collapse.join(out)

    return Array(out, dtype=str)


@register_func(None, context=Context.EVAL)
def paste0(*args: StringOrIter, collapse: str = None) -> StringOrIter:
    """`paste` with `sep=""`

    See [paste](datar.base.string.paste)
    """
    return paste(*args, sep="", collapse=collapse)


# sprintf ----------------------------------------------------------------


@register_func(None, context=Context.EVAL)
def sprintf(fmt: StringOrIter, *args: StringOrIter) -> StringOrIter:
    """C-style String Formatting

    Args:
        fmt: The formats
        *args: The values

    Returns:
        A scalar string if all fmt, *args are scalar strings, otherwise
        an array of formatted strings
    """
    if is_scalar(fmt) and all(is_scalar(arg) for arg in args):
        return fmt % args

    # args = [as_character(arg, _na='NA') for arg in args]
    maxlen = max(map(length_of, args))
    maxlen = max(maxlen, max(map(length_of, fmt)))

    fmt = recycle_value(fmt, maxlen)
    args = [recycle_value(arg, maxlen) for arg in args]
    return Array([fmt[i] % arg for i, arg in enumerate(zip(*args))], dtype=str)


# substr, substring ----------------------------------


@register_func(None, context=Context.EVAL)
def substr(
    x: StringOrIter,
    start: IntOrIter,
    stop: IntOrIter,
    base0_: bool = None,
) -> StringOrIter:
    """Extract substrings in strings.

    Args:
        x: The strings
        start: The start positions to extract
        stop: The stop positions to extract
        base0_: Whether `start` and `stop` are 0-based
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The substrings from `x`
    """
    if is_scalar(x) and is_scalar(start) and is_scalar(stop):
        if is_null(x):
            return NA
        base0_ = get_option("index.base.0", base0_)
        x = as_character(x)
        lenx = len(x)
        # int() converts numpy.int64 to int
        start0 = position_at(int(start), lenx, base0=base0_)
        stop0 = position_at(
            min(int(stop), lenx - int(base0_)), lenx, base0=base0_
        )
        return x[start0 : stop0 + 1]

    if is_scalar(x):
        x = [x] # type: ignore
    if is_scalar(start):
        start = [start] # type: ignore
    if is_scalar(stop):
        stop = [stop] # type: ignore
    maxlen = max(length_of(x), length_of(start), length_of(stop))
    x = recycle_value(x, maxlen)
    start = recycle_value(start, maxlen)
    stop = recycle_value(stop, maxlen)
    out = [
        substr(elem, start_, stop_, base0_)
        for elem, start_, stop_ in zip(x, start, stop)
    ]
    if is_null(out).any():
        return Array(out, dtype=object)
    return Array(out, dtype=str)


@register_func(None, context=Context.EVAL)
def substring(
    x: StringOrIter,
    first: IntOrIter,
    last: IntOrIter = 1000000,
    base0_: bool = None,
) -> StringOrIter:
    """Extract substrings in strings.

    Args:
        x: The strings
        start: The start positions to extract
        stop: The stop positions to extract
        base0_: Whether `start` and `stop` are 0-based
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The substrings from `x`
    """
    return substr(x, first, last, base0_)


# strsplit --------------------------------


@register_func(None, context=Context.EVAL)
def strsplit(
    x: StringOrIter, split: StringOrIter, fixed: bool = False
) -> Iterable[Union[str, Iterable[str]]]:
    """Split strings by separator

    Args:
        x: The strings. Have to be strings, no casting will be done.
        split: The separators to split
        fixed: fixed matching (instead of regex matching)?

    Returns:
        List of split strings of x if both x and split are scalars. Otherwise,
        an array of split strings
    """
    if is_scalar(x) and is_scalar(split):
        if fixed:
            split = re.escape(split)
        split = re.compile(split)
        return split.split(x)

    maxlen = max(length_of(x), length_of(split))
    x = recycle_value(x, maxlen)
    split = recycle_value(split, maxlen)
    out = [strsplit(elem, splt, fixed=fixed) for elem, splt in zip(x, split)]
    return Array(out, dtype=object)
