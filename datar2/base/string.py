"""String functions in R"""
from functools import singledispatch
import re
from typing import Any, Iterable, Tuple, Union

import numpy as np
import pandas as pd
from pandas import Series
from pandas._typing import Dtype, AnyArrayLike
from pandas.api.types import is_string_dtype, is_scalar
from pandas.core.groupby import SeriesGroupBy
from pipda import register_func

from datar2.core.tibble import TibbleRowwise

from ..core.contexts import Context
from ..core.utils import (
    arg_match,
    ensure_nparray,
    logger,
    transform_func_decor,
    regcall,
)
from ..tibble import tibble
from .casting import _as_type
from .testing import _register_type_testing
from .seq import lengths


def _recycle_value(value: Any, size: int, name: str = None) -> np.ndarray:
    """Recycle a value based on a dataframe
    Args:
        value: The value to be recycled
        size: The size to recycle to
    Returns:
        The recycled value
    """
    name = name or "value"
    value = ensure_nparray(value)

    if value.size > 0 and size % value.size != 0:
        raise ValueError(
            f"Cannot recycle {name} (size={value.size}) to size {size}."
        )

    if value.size == size == 0:
        return np.array([], dtype=object)

    if value.size == 0:
        value = np.array([np.nan], dtype=object)

    return value.repeat(size // value.size)


@register_func(None, context=Context.EVAL)
def as_character(
    x: Any,
    str_dtype: Dtype = str,
    _na: Any = np.nan,
) -> AnyArrayLike:
    """Convert an object or elements of an iterable into string

    Aliases `as_str` and `as_string`

    Args:
        x: The object
        str_dtype: The string dtype to convert to
        _na: How NAs should be casted. Specify np.nan will keep them unchanged.
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
    scalar_types=(str, np.str_),
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
    pattern: AnyArrayLike,
    x: AnyArrayLike,
    ignore_case: bool = False,
    value: bool = False,
    fixed: bool = False,
    invert: bool = False,
) -> Iterable[Union[int, str]]:
    """R's grep, get the element in x matching the pattern

    Args:
        pattern: The pattern
        x: A string or an iterable of strings; or those can be coerced to
        ignore_case: Do case-insensitive matching?
        value: Return values instead of indices?
        fixed: Fixed matching (instead of regex matching)?
        invert: Return elements thata don't match instead?

    Returns:
        The matched (or unmatched (`invert=True`)) indices
        (or values (`value=True`)).
    """
    if is_scalar(x) or (
        not isinstance(x, Series) and not isinstance(x, SeriesGroupBy)
    ):
        x = ensure_nparray(x)

    matched = grepl(
        pattern=pattern,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
        invert=invert,
    )

    if value:
        if isinstance(x, Series):
            return x[matched.values]
        if isinstance(x, SeriesGroupBy):
            return x.obj[matched.obj.values].groupby(x.grouper)
        return x[matched]

    return np.flatnonzero(matched)


@register_func(None, context=Context.EVAL)
def grepl(
    pattern: AnyArrayLike,
    x: Iterable,
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
    if isinstance(x, Series):
        return x.transform(
            _match,
            pattern=pattern,
            ignore_case=ignore_case,
            fixed=fixed,
            invert=invert,
        )

    if isinstance(x, SeriesGroupBy):
        out = x.obj.transform(
            _match,
            pattern=pattern,
            ignore_case=ignore_case,
            fixed=fixed,
            invert=invert,
        ).groupby(x.grouper)
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    return _match(
        pattern,
        x,
        ignore_case=ignore_case,
        invert=invert,
        fixed=fixed,
    )


@register_func(None, context=Context.EVAL)
def sub(
    pattern: AnyArrayLike,
    replacement: AnyArrayLike,
    x: str,
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
    if isinstance(x, Series):
        return x.transform(
            _sub,
            pattern=pattern,
            replacement=replacement,
            ignore_case=ignore_case,
            fixed=fixed,
        )

    if isinstance(x, SeriesGroupBy):
        out = x.obj.transform(
            _sub,
            pattern=pattern,
            replacement=replacement,
            ignore_case=ignore_case,
            fixed=fixed,
        ).groupby(x.grouper)
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

    return _sub(
        pattern=pattern,
        replacement=replacement,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
    )


@register_func(None, context=Context.EVAL)
def gsub(
    pattern: str,
    replacement: str,
    x: Iterable,
    ignore_case: bool = False,
    fixed: bool = False,
) -> Iterable[str]:
    """R's gsub, replace a pattern with replacement for elements in x,
    each for all matched parts

    See Also:
        [sub()](datar.base.string.sub)
    """
    if isinstance(x, Series):
        return x.transform(
            _sub,
            pattern=pattern,
            replacement=replacement,
            ignore_case=ignore_case,
            fixed=fixed,
            count=0,
            fun="gsub",
        )

    if isinstance(x, SeriesGroupBy):
        out = x.obj.transform(
            _sub,
            pattern=pattern,
            replacement=replacement,
            ignore_case=ignore_case,
            fixed=fixed,
            count=0,
            fun="gsub",
        ).groupby(x.grouper)
        if getattr(x, "is_rowwise", False):
            out.is_rowwise = True
        return out

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
    pattern: AnyArrayLike, fun: str, arg: str = "pattern"
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
    if pd.isnull(text):
        return False

    flags = re.IGNORECASE if ignore_case else 0
    if fixed:
        pattern = re.escape(pattern)

    pattern = re.compile(pattern, flags)
    matched = pattern.search(text)
    if invert:
        matched = not bool(matched)
    return bool(matched)


_match = np.vectorize(_match, excluded={"pattern"})


def _sub(
    pattern: str,
    replacement: str,
    x: str,
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

    return pattern.sub(repl=replacement, count=count, string=x)


_sub = np.vectorize(_sub, excluded={"pattern", "replacement"})


@transform_func_decor(vectorized=False)
def nchar(
    x: str,
    type: str = "chars",
    allow_na: bool = True,  # i.e.: '\ud861'
    keep_na: bool = None,
    _na_len: int = 2,
) -> Iterable[int]:
    """Get the size of the elements in x"""
    x, keep_na = _prepare_nchar(x, type, keep_na)
    out = _nchar_scalar(
        x, retn=type, allow_na=allow_na, keep_na=keep_na, na_len=_na_len
    )
    if pd.isnull(out):
        return np.nan
    return out


@transform_func_decor(vectorized=False)
def nzchar(x, na_as: bool = False) -> bool:
    """Find out if elements of a character vector are non-empty strings.

    Args:
        x: Strings to test
        na_as: What to return when for NA's

    Returns:
        A bool array to tell whether elements in x are non-empty strings
    """
    x = as_character(x, _na="a" if na_as else "")
    if pd.isnull(x):
        return np.nan

    return bool(x)


# nchar helpers --------------------------------


def _prepare_nchar(
    x: str, type: str, keep_na: bool
) -> Tuple[Iterable[str], bool]:
    """Prepare arguments for n(z)char"""
    arg_match(type, "type", ["chars", "bytes", "width"])
    if keep_na is None:
        keep_na = type != "width"

    return as_character(x), keep_na


def _nchar_scalar(
    x: str, retn: str, allow_na: bool, keep_na: bool, na_len: int
) -> int:
    """Get the size of a scalar string"""
    if pd.isnull(x):
        return np.nan if keep_na else na_len

    if retn == "width":
        from wcwidth import wcswidth

        return wcswidth(x)
    if retn == "chars":
        return len(x)

    try:
        x = x.encode("utf-8")
    except UnicodeEncodeError:
        if allow_na:
            return np.nan
        raise
    return len(x)


# paste and paste0 --------------------


@register_func(None, context=Context.EVAL)
def paste(
    *args: AnyArrayLike, sep: str = " ", collapse: str = None
) -> AnyArrayLike:
    """Concatenate vectors after converting to character.

    Args:
        *args: strings to be concatenated
        sep: The separator
        collapse: The separator to collapse the final string arrays

    Returns:
        A single string if collapse is given, otherwise an array of strings.
    """
    if len(args) == 1 and isinstance(args[0], TibbleRowwise):
        return args[0].apply(
            lambda row: paste(*row, sep=sep, collapse=collapse), axis=1
        )

    maxlen = max(regcall(lengths, args))
    args = zip(
        *(
            _recycle_value(arg, maxlen, f"{i}th value")
            for i, arg in enumerate(args)
        )
    )

    args = [as_character(arg, _na="NA") for arg in args]
    out = [sep.join(arg) for arg in args]
    if collapse is not None:
        return collapse.join(out)

    return np.array(out, dtype=object)


paste0 = lambda *args, collapse=None: paste(*args, sep="", collapse=collapse)


# sprintf ----------------------------------------------------------------


@register_func(None, context=Context.EVAL)
def sprintf(fmt: AnyArrayLike, *args: AnyArrayLike) -> AnyArrayLike:
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

    df = tibble(fmt, *args)
    return df.apply(lambda row: row[0] % row[1:], axis=1).values


# substr, substring ----------------------------------


@transform_func_decor(vectorized=False, otypes=[object])
def substr(
    x: str,
    start: int,
    stop: int,
) -> str:
    """Extract substrings in strings.

    Args:
        x: The strings
        start: The start positions to extract
        stop: The stop positions to extract

    Returns:
        The substrings from `x`
    """
    if pd.isnull(x):
        return np.nan

    return str(x)[start:stop]


@transform_func_decor(vectorized=False, otypes=[object])
def substring(
    x: str,
    first: int,
    last: int = 1000000,
) -> AnyArrayLike:
    """Extract substrings in strings.

    Args:
        x: The strings
        start: The start positions to extract
        stop: The stop positions to extract

    Returns:
        The substrings from `x`
    """
    if pd.isnull(x):
        return np.nan

    return str(x)[first:last]


# strsplit --------------------------------


@transform_func_decor(vectorized=False, otypes=[object])
def strsplit(x: str, split: str, fixed: bool = False) -> AnyArrayLike:
    """Split strings by separator

    Args:
        x: The strings. Have to be strings, no casting will be done.
        split: The separators to split
        fixed: fixed matching (instead of regex matching)?

    Returns:
        List of split strings of x if both x and split are scalars. Otherwise,
        an array of split strings
    """
    if fixed:
        split = re.escape(split)
    split = re.compile(split)
    return split.split(x)


# startsWith, endsWith
@transform_func_decor(vectorized=False)
def startswith(x: str, prefix: str) -> str:
    """Determines if entries of x start with prefix

    Args:
        x: A vector of strings or a string
        prefix: The prefix to test against

    Returns:
        A bool vector for each element in x if element startswith the prefix
    """
    return str(x).startswith(prefix)


@transform_func_decor(vectorized=False)
def endswith(x, suffix) -> str:
    """Determines if entries of x end with suffix

    Args:
        x: A vector of strings or a string
        suffix: The suffix to test against

    Returns:
        A bool vector for each element in x if element endswith the suffix
    """
    return str(x).endswith(suffix)


@transform_func_decor(vectorized=False)
def strtoi(x: str, base: int = 0):
    """Convert strings to integers according to the given base

    Args:
        x: A string or vector of strings
        base: an integer which is between 2 and 36 inclusive, or zero.
            With zero, a suitable base will be chosen following the C rules.

    Returns:
        Converted integers
    """
    return int(x, base=base)


def _chartr_vec(x: str, old: str, new: str):
    for oldc, newc in zip(old, new):
        x = x.replace(oldc, newc)
    return x


_chartr_vec = np.vectorize(
    _chartr_vec, otypes=[object], excluded={"old", "new"}
)


@singledispatch
def _chartr(x, old, new):
    new = new[: len(old)]
    return _chartr_vec(x, old, new)


@_chartr.register(Series)
def _(x, old, new):
    return Series(_chartr(x.values, old, new), index=x.index, name=x.name)


@_chartr.register(SeriesGroupBy)
def _(x, old, new):
    out = _chartr(x.obj, old, new).groupby(x.grouper)
    if getattr(x, "is_rowwise", False):
        out.is_rowwise = True

    return out


@register_func(None, context=Context.EVAL)
def chartr(old, new, x) -> str:
    """Replace strings char by char

    Args:
        x: A string or vector of strings
        old: A set of characters to replace
        new: A set of characters to replace with

    Returns:
        The strings in x being replaced
    """
    old = _warn_more_pat_or_rep(old, "chartr", "old")
    new = _warn_more_pat_or_rep(new, "chartr", "new")
    if len(old) > len(new):
        raise ValueError("'old' is longer than 'new'")

    new = new[: len(old)]
    return _chartr(x, old, new)


@transform_func_decor(vectorized=False)
def tolower(x: AnyArrayLike) -> AnyArrayLike:
    """Convert strings to lower case

    Args:
        x: A string or vector of strings

    Returns:
        Converted strings
    """
    return str(x).lower()


@transform_func_decor(vectorized=False)
def toupper(x):
    """Convert strings to upper case

    Args:
        x: A string or vector of strings

    Returns:
        Converted strings
    """
    return str(x).upper()


@transform_func_decor(vectorized=False)
def trimws(x, which: str = "both", whitespace: str = r"[ \t\r\n]"):
    """Remove leading and/or trailing whitespace from character strings.

    Args:
        x: A string or vector of strings
        which: A character string specifying whether to remove
            both leading and trailing whitespace (default),
            or only leading ("left") or trailing ("right").
        whitespace: a string specifying a regular expression to
            match (one character of) “white space”

    Returns:
        The strings with whitespaces removed
    """
    which = arg_match(which, "which", ["both", "left", "right"])
    x = str(x)

    if which == "both":
        expr = f"^{whitespace}|{whitespace}$"
    elif which == "left":
        expr = f"^{whitespace}"
    else:
        expr = f"{whitespace}$"

    return re.sub(expr, "", x)
