"""String functions in R"""
import re

import numpy as np
import pandas as pd
from pandas import Series
from pandas.core.groupby import SeriesGroupBy
from pandas.api.types import is_string_dtype, is_scalar
from pipda import register_func


from ..core.tibble import TibbleRowwise
from ..core.contexts import Context
from ..core.factory import func_factory, dispatching
from ..core.utils import (
    arg_match,
    ensure_nparray,
    logger,
    regcall,
)
from .casting import _as_type
from .testing import _register_type_testing
from .logical import as_logical
from .seq import lengths


def _recycle_value(value, size, name=None):
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
    x,
    str_dtype=str,
    _na=np.nan,
):
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
@dispatching(kind="transform", qualname="datar.base.grep")
def _grep(
    x, pattern, ignore_case=False, value=False, fixed=False, invert=False
):
    matched = _grepl.dispatch(Series)(
        x,
        pattern,
        ignore_case=ignore_case,
        fixed=fixed,
        invert=invert,
    )

    if value:
        return x[matched]

    return np.flatnonzero(matched)


@register_func(None, context=Context.EVAL)
def grep(
    pattern,
    x,
    ignore_case=False,
    value=False,
    fixed=False,
    invert=False,
):
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
    return _grep(
        x if isinstance(x, (Series, SeriesGroupBy)) else Series(x),
        pattern,
        ignore_case=ignore_case,
        value=value,
        fixed=fixed,
        invert=invert,
    )


@dispatching(kind="transform", qualname="datar.base.grepl")
def _grepl(x, pattern, ignore_case, fixed, invert):
    pattern = _warn_more_pat_or_rep(pattern, "grepl")
    return _match(
        x,
        pattern,
        ignore_case=ignore_case,
        invert=invert,
        fixed=fixed,
    )


@register_func(None, context=Context.EVAL)
def grepl(
    pattern,
    x,
    ignore_case=False,
    fixed=False,
    invert=False,
):
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
    return _grepl(
        x if isinstance(x, (Series, SeriesGroupBy)) else Series(x),
        pattern,
        ignore_case=ignore_case,
        fixed=fixed,
        invert=invert,
    )


@dispatching(kind="transform", qualname="datar.base.sub")
def _sub(x, pattern, replacement, ignore_case, fixed):
    return _sub_(
        pattern=pattern,
        replacement=replacement,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
    )


@register_func(None, context=Context.EVAL)
def sub(
    pattern,
    replacement,
    x,
    ignore_case=False,
    fixed=False,
):
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
        x if isinstance(x, (Series, SeriesGroupBy)) else Series(x),
        pattern,
        replacement,
        ignore_case=ignore_case,
        fixed=fixed,
    )


@dispatching(kind="transform", qualname="datar.base.gsub")
def _gsub(x, pattern, replacement, ignore_case, fixed):
    return _sub_(
        pattern=pattern,
        replacement=replacement,
        x=x,
        ignore_case=ignore_case,
        fixed=fixed,
        count=0,
        fun="gsub",
    )


@register_func(None, context=Context.EVAL)
def gsub(
    pattern,
    replacement,
    x,
    ignore_case=False,
    fixed=False,
):
    """R's gsub, replace a pattern with replacement for elements in x,
    each for all matched parts

    See Also:
        [sub()](datar.base.string.sub)
    """
    return _gsub(
        x if isinstance(x, (Series, SeriesGroupBy)) else Series(x),
        pattern,
        replacement,
        ignore_case=ignore_case,
        fixed=fixed,
    )


# Grep family helpers --------------------------------


def _warn_more_pat_or_rep(pattern, fun, arg="pattern"):
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


def _match(text, pattern, ignore_case, invert, fixed):
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


def _sub_(
    pattern,
    replacement,
    x,
    ignore_case=False,
    fixed=False,
    count=1,
    fun="sub",
):
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


_sub_ = np.vectorize(_sub_, excluded={"pattern", "replacement"})


@func_factory("transform", "x")
def nchar(
    x,
    type="chars",
    allow_na=True,  # i.e.: '\ud861'
    keep_na=None,
    _na_len=2,
):
    """Get the size of the elements in x"""
    x, keep_na = _prepare_nchar(x, type, keep_na)
    return _nchar_scalar(
        x, retn=type, allow_na=allow_na, keep_na=keep_na, na_len=_na_len
    )


@func_factory("transform", "x")
def nzchar(x, keep_na=False):
    """Find out if elements of a character vector are non-empty strings.

    Args:
        x: Strings to test
        keep_na: What to return when for NA's

    Returns:
        A bool array to tell whether elements in x are non-empty strings
    """
    x = regcall(as_character, x, _na=np.nan if keep_na else "")
    if not keep_na:
        return x.fillna(False).astype(bool)
    return as_logical(x, na=np.nan)


# nchar helpers --------------------------------


def _prepare_nchar(x, type, keep_na):
    """Prepare arguments for n(z)char"""
    arg_match(type, "type", ["chars", "bytes", "width"])
    if keep_na is None:
        keep_na = type != "width"

    return regcall(as_character, x), keep_na


@np.vectorize
def _nchar_scalar(x, retn, allow_na, keep_na, na_len):
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
def paste(*args, sep=" ", collapse=None):
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


@register_func(None, context=Context.EVAL)
def paste0(*args, sep="", collapse=None):
    """Paste with empty string as sep"""
    return regcall(paste, *args, sep="", collapse=collapse)


# sprintf ----------------------------------------------------------------


@func_factory("transform", {"fmt"})
def sprintf(fmt, *args):
    """C-style String Formatting

    Args:
        fmt: The formats
        *args: The values

    Returns:
        A scalar string if all fmt, *args are scalar strings, otherwise
        an array of formatted strings
    """

    return fmt.transform(lambda x: np.nan if pd.isnull(x) else str(x) % args)


# substr, substring ----------------------------------


@func_factory("transform", "x")
def substr(x, start, stop):
    """Extract substrings in strings.

    Args:
        x: The strings
        start: The start positions to extract
        stop: The stop positions to extract

    Returns:
        The substrings from `x`
    """
    x = regcall(as_character, x)
    return x.str[start:stop]


@func_factory("transform", "x")
def substring(x, first, last=1000000):
    """Extract substrings in strings.

    Args:
        x: The strings
        start: The start positions to extract
        stop: The stop positions to extract

    Returns:
        The substrings from `x`
    """
    x = regcall(as_character, x)
    return x.str[first:last]


# strsplit --------------------------------


@func_factory("transform", {"x", "split"})
def strsplit(x, split, fixed=False):
    """Split strings by separator

    Args:
        x: The strings. Have to be strings, no casting will be done.
        split: The separators to split
        fixed: fixed matching (instead of regex matching)?

    Returns:
        List of split strings of x if both x and split are scalars. Otherwise,
        an array of split strings
    """

    def split_str(string, sep):
        if fixed:
            return string.split(sep)

        sep = re.compile(sep)
        return sep.split(string)

    return np.vectorize(split_str, [object])(x, split)


# startsWith, endsWith
@func_factory("transform", "x")
def startswith(x, prefix):
    """Determines if entries of x start with prefix

    Args:
        x: A vector of strings or a string
        prefix: The prefix to test against

    Returns:
        A bool vector for each element in x if element startswith the prefix
    """
    x = regcall(as_character, x)
    return x.str.startswith(prefix)


@func_factory("transform", "x")
def endswith(x, suffix):
    """Determines if entries of x end with suffix

    Args:
        x: A vector of strings or a string
        suffix: The suffix to test against

    Returns:
        A bool vector for each element in x if element endswith the suffix
    """
    x = regcall(as_character, x)
    return x.str.endswith(suffix)


@func_factory("transform", "x")
def strtoi(x, base=0):
    """Convert strings to integers according to the given base

    Args:
        x: A string or vector of strings
        base: an integer which is between 2 and 36 inclusive, or zero.
            With zero, a suitable base will be chosen following the C rules.

    Returns:
        Converted integers
    """
    return x.transform(int, base=base)


@func_factory("transform", "x")
def chartr(old, new, x):
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
    for oldc, newc in zip(old, new):
        x = x.str.replace(oldc, newc)
    return x


@func_factory("transform", "x")
def tolower(x):
    """Convert strings to lower case

    Args:
        x: A string or vector of strings

    Returns:
        Converted strings
    """
    x = regcall(as_character, x)
    return x.str.lower()


@func_factory("transform", "x")
def toupper(x):
    """Convert strings to upper case

    Args:
        x: A string or vector of strings

    Returns:
        Converted strings
    """
    x = regcall(as_character, x)
    return x.str.upper()


@func_factory("transform", "x")
def trimws(x, which="both", whitespace=r"[ \t\r\n]"):
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

    x = regcall(as_character, x)

    if which == "both":
        expr = f"^{whitespace}|{whitespace}$"
    elif which == "left":
        expr = f"^{whitespace}"
    else:
        expr = f"{whitespace}$"

    return np.vectorize(re.sub, excluded={"pattern", "repl"})(expr, "", x)
