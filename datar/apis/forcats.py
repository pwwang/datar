from pipda import register_func as _register_func

from ..core.utils import (
    NotImplementedByCurrentBackendError as _NotImplementedByCurrentBackendError,
)
from .base import as_factor  # noqa: F401


@_register_func(pipeable=True, dispatchable=True)
def fct_relevel(_f, *lvls, after: int = None):
    """Reorder factor levels by hand

    Args:
        _f: A factor (categoriccal), or a string vector
        *lvls: Either a function (then `len(lvls)` should equal to `1`) or
            the new levels.
            A function will be called with the current levels as input, and the
            return value (which must be a character vector) will be used to
            relevel the factor.
            Any levels not mentioned will be left in their existing order,
            by default after the explicitly mentioned levels.
        after: Where should the new values be placed?

    Returns:
        The factor with levels replaced
    """
    raise _NotImplementedByCurrentBackendError("fct_relevel", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_inorder(_f, ordered: bool = None):
    """Reorder factor levels by first appearance

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    raise _NotImplementedByCurrentBackendError("fct_inorder", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_infreq(_f, ordered: bool = None):
    """Reorder factor levels by frequency

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    raise _NotImplementedByCurrentBackendError("fct_infreq", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_inseq(_f, ordered: bool = None):
    """Reorder factor levels by sequence

    Args:
        _f: A factor
        ordered: A logical which determines the "ordered" status of the
            output factor.

    Returns:
        The factor with levels reordered
    """
    raise _NotImplementedByCurrentBackendError("fct_inseq", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_reorder(_f, _x, *args, _fun=None, _desc: bool = False, **kwargs):
    """Reorder factor levels by a function (default: median)

    Args:
        _f: A factor
        _x: The data to be used to reorder the factor
        _fun: A function to be used to reorder the factor
        _desc: If `True`, the factor will be reordered in descending order
        *args: Extra arguments to be passed to `_fun`
        **kwargs: Extra keyword arguments to be passed to `_fun`

    Returns:
        The factor with levels reordered
    """
    raise _NotImplementedByCurrentBackendError("fct_reorder", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_reorder2(_f, _x, *args, _fun=None, _desc: bool = False, **kwargs):
    """Reorder factor levels by a function (default: `last2`)

    Args:
        _f: A factor
        _x: The data to be used to reorder the factor
        _fun: A function to be used to reorder the factor
        _desc: If `True`, the factor will be reordered in descending order
        *args: Extra arguments to be passed to `_fun`
        **kwargs: Extra keyword arguments to be passed to `_fun`

    Returns:
        The factor with levels reordered
    """
    raise _NotImplementedByCurrentBackendError("fct_reorder2", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_shuffle(_f):
    """Shuffle the levels of a factor

    Args:
        _f: A factor

    Returns:
        The factor with levels shuffled
    """
    raise _NotImplementedByCurrentBackendError("fct_shuffle", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_rev(_f):
    """Reverse the order of the levels of a factor

    Args:
        _f: A factor

    Returns:
        The factor with levels reversed
    """
    raise _NotImplementedByCurrentBackendError("fct_rev", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_shift(_f, n: int = 1):
    """Shift the levels of a factor

    Args:
        _f: A factor
        n: The number of levels to shift

    Returns:
        The factor with levels shifted
    """
    raise _NotImplementedByCurrentBackendError("fct_shift", _f)


@_register_func(pipeable=True, dispatchable=True)
def first2(_x, _y):
    """Find the first element of `_y` ordered by `_x`

    Args:
        _x: The vector used to order `_y`
        _y: The vector to get the first element of

    Returns:
        First element of `_y` ordered by `_x`
    """
    raise _NotImplementedByCurrentBackendError("first2", _x)


@_register_func(pipeable=True, dispatchable=True)
def last2(_x, _y):
    """Find the last element of `_y` ordered by `_x`

    Args:
        _x: The vector used to order `_y`
        _y: The vector to get the last element of

    Returns:
        Last element of `_y` ordered by `_x`
    """
    raise _NotImplementedByCurrentBackendError("last2", _x)


@_register_func(pipeable=True, dispatchable=True)
def fct_anon(_f, prefix: str = ""):
    """Anonymise factor levels

    Args:
        f: A factor.
        prefix: A character prefix to insert in front of the random labels.

    Returns:
        The factor with levels anonymised
    """
    raise _NotImplementedByCurrentBackendError("fct_anon", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_recode(_f, *args, **kwargs):
    """Change factor levels by hand

    Args:
        _f: A factor
        *args: and
        **kwargs: A sequence of named character vectors where the name
            gives the new level, and the value gives the old level.
            Levels not otherwise mentioned will be left as is. Levels can
            be removed by naming them `NULL`.
            As `NULL/None` cannot be a name of keyword arguments, replacement
            has to be specified as a dict
            (i.e. `fct_recode(x, {NULL: "apple"})`)
            If you want to replace multiple values with the same old value,
            use a `set`/`list`/`numpy.ndarray`
            (i.e. `fct_recode(x, fruit=["apple", "banana"])`).
            This is a safe way, since `set`/`list`/`numpy.ndarray` is
            not hashable to be a level of a factor.
            Do NOT use a `tuple`, as it's hashable!

            Note that the order of the name-value is in the reverse way as
            `dplyr.recode()` and `dplyr.recode_factor()`

    Returns:
        The factor recoded with given recodings
    """
    raise _NotImplementedByCurrentBackendError("fct_recode", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_collapse(_f, other_level=None, **kwargs):
    """Collapse factor levels into manually defined groups

    Args:
        _f: A factor
        **kwargs: The levels to collapse.
            Like `name=[old_level, old_level1, ...]`. The old levels will
            be replaced with `name`
        other_level: Replace all levels not named in `kwargs`.
            If not, don't collapse them.

    Returns:
        The factor with levels collapsed.
    """
    raise _NotImplementedByCurrentBackendError("fct_collapse", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_lump(_f, n, prop, w, other_level="Other", ties_method: str = "min"):
    """Lump together factor levels into "other"

    Args:
        f: A factor
        n: Positive `n` preserves the most common `n` values.
            Negative `n` preserves the least common `-n` values.
            It there are ties, you will get at least `abs(n)` values.
        prop: Positive `prop` lumps values which do not appear at least
            `prop` of the time. Negative `prop` lumps values that
            do not appear at most `-prop` of the time.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.
        ties_method A character string specifying how ties are treated.
            One of: `average`, `first`, `dense`, `max`, and `min`.

    Returns:
        The factor with levels lumped.
    """
    raise _NotImplementedByCurrentBackendError("fct_lump", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_lump_min(_f, min_, w=None, other_level="Other"):
    """lumps levels that appear fewer than `min_` times.

    Args:
        _f: A factor
        min_: Preserve levels that appear at least `min_` number of times.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels lumped.
    """
    raise _NotImplementedByCurrentBackendError("fct_lump_min", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_lump_prop(_f, prop, w=None, other_level="Other"):
    """Lumps levels that appear in fewer `prop * n` times.

    Args:
        _f: A factor
        prop: Positive `prop` lumps values which do not appear at least
            `prop` of the time. Negative `prop` lumps values that
            do not appear at most `-prop` of the time.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels lumped.
    """
    raise _NotImplementedByCurrentBackendError("fct_lump_prop", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_lump_n(_f, n, w=None, other_level="Other"):
    """Lumps all levels except for the `n` most frequent.

    Args:
        f: A factor
        n: Positive `n` preserves the most common `n` values.
            Negative `n` preserves the least common `-n` values.
            It there are ties, you will get at least `abs(n)` values.
        w: An optional numeric vector giving weights for frequency of
            each value (not level) in f.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.
        ties_method A character string specifying how ties are treated.
            One of: `average`, `first`, `dense`, `max`, and `min`.

    Returns:
        The factor with levels lumped.
    """
    raise _NotImplementedByCurrentBackendError("fct_lump_n", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_lump_lowfreq(_f, other_level="Other"):
    """lumps together the least frequent levels, ensuring
    that "other" is still the smallest level.

    Args:
        f: A factor
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels lumped.
    """
    raise _NotImplementedByCurrentBackendError("fct_lump_lowfreq", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_other(_f, keep=None, drop=None, other_level="Other"):
    """Replace levels with "other"

    Args:
        _f: A factor
        keep: and
        drop: Pick one of `keep` and `drop`:
            - `keep` will preserve listed levels, replacing all others with
                `other_level`.
            - `drop` will replace listed levels with `other_level`, keeping all
                as is.
        other_level: Value of level used for "other" values. Always
            placed at end of levels.

    Returns:
        The factor with levels replaced.
    """
    raise _NotImplementedByCurrentBackendError("fct_other", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_relabel(_f, _fun, *args, **kwargs):
    """Automatically relabel factor levels, collapse as necessary

    Args:
        _f: A factor
        _fun: A function to be applied to each level. Must accept the old
            levels and return a character vector of the same length
            as its input.
        *args: and
        **kwargs: Addtional arguments to `_fun`

    Returns:
        The factor with levels relabeled
    """
    raise _NotImplementedByCurrentBackendError("fct_relabel", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_expand(_f, *additional_levels):
    """Add additional levels to a factor

    Args:
        _f: A factor
        *additional_levels: Additional levels to add to the factor.
            Levels that already exist will be silently ignored.

    Returns:
        The factor with levels expanded
    """
    raise _NotImplementedByCurrentBackendError("fct_expand", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_explicit_na(_f, na_level="(Missing)"):
    """Make missing values explicit

    This gives missing values an explicit factor level, ensuring that they
    appear in summaries and on plots.

    Args:
        _f: A factor
        na_level: Level to use for missing values.
            This is what NAs will be changed to.

    Returns:
        The factor with explict na_levels
    """
    raise _NotImplementedByCurrentBackendError("fct_explicit_na", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_drop(_f, only=None):
    """Drop unused levels

    Args:
        _f: A factor
        only: A character vector restricting the set of levels to be dropped.
            If supplied, only levels that have no entries and appear in
            this vector will be removed.

    Returns:
        The factor with unused levels dropped
    """
    raise _NotImplementedByCurrentBackendError("fct_drop", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_unify(
    fs,
    levels=None,
):
    """Unify the levels in a list of factors

    Args:
        fs: A list of factors
        levels: Set of levels to apply to every factor. Default to union
            of all factor levels

    Returns:
        A list of factors with the levels expanded
    """
    raise _NotImplementedByCurrentBackendError("fct_unify", fs)


@_register_func(pipeable=True, dispatchable=True)
def fct_c(*fs):
    """Concatenate factors, combining levels

    This is a useful ways of patching together factors from multiple sources
    that really should have the same levels but don't.

    Args:
        *fs: factors to concatenate

    Returns:
        The concatenated factor
    """
    raise _NotImplementedByCurrentBackendError("fct_c")


@_register_func(pipeable=True, dispatchable=True)
def fct_cross(
    *fs,
    sep: str = ":",
    keep_empty: bool = False,
):
    """Combine levels from two or more factors to create a new factor

    Computes a factor whose levels are all the combinations of
    the levels of the input factors.

    Args:
        *fs: factors to cross
        sep: A string to separate levels
        keep_empty: If True, keep combinations with no observations as levels

    Returns:
        The new factor
    """
    raise _NotImplementedByCurrentBackendError("fct_cross")


@_register_func(pipeable=True, dispatchable=True)
def fct_count(_f, sort: bool = False, prop=False):
    """Count entries in a factor

    Args:
        _f: A factor
        sort: If True, sort the result so that the most common values float to
            the top
        prop: If True, compute the fraction of marginal table.

    Returns:
        A data frame with columns `f`, `n` and `p`, if prop is True
    """
    raise _NotImplementedByCurrentBackendError("fct_count", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_match(_f, lvls):
    """Test for presence of levels in a factor

    Do any of `lvls` occur in `_f`?

    Args:
        _f: A factor
        lvls: A vector specifying levels to look for.

    Returns:
        A logical factor
    """
    raise _NotImplementedByCurrentBackendError("fct_match", _f)


@_register_func(pipeable=True, dispatchable=True)
def fct_unique(_f):
    """Unique values of a factor

    Args:
        _f: A factor

    Returns:
        The factor with the unique values in `_f`
    """
    raise _NotImplementedByCurrentBackendError("fct_unique", _f)


@_register_func(pipeable=True, dispatchable=True)
def lvls_reorder(
    _f,
    idx,
    ordered: bool = None,
):
    """Leaves values of a factor as they are, but changes the order by
    given indices

    Args:
        f: A factor (or character vector).
        idx: A integer index, with one integer for each existing level.
        new_levels: A character vector of new levels.
        ordered: A logical which determines the "ordered" status of the
          output factor. `None` preserves the existing status of the factor.

    Returns:
        The factor with levels reordered
    """
    raise _NotImplementedByCurrentBackendError("lvls_reorder", _f)


@_register_func(pipeable=True, dispatchable=True)
def lvls_revalue(
    _f,
    new_levels,
):
    """changes the values of existing levels; there must
    be one new level for each old level

    Args:
        _f: A factor
        new_levels: A character vector of new levels.

    Returns:
        The factor with the new levels
    """
    raise _NotImplementedByCurrentBackendError("lvls_revalue", _f)


@_register_func(pipeable=True, dispatchable=True)
def lvls_expand(
    _f,
    new_levels,
):
    """Expands the set of levels; the new levels must
    include the old levels.

    Args:
        _f: A factor
        new_levels: The new levels. Must include the old ones

    Returns:
        The factor with the new levels
    """
    raise _NotImplementedByCurrentBackendError("lvls_expand", _f)


@_register_func(pipeable=True, dispatchable=True)
def lvls_union(fs):
    """Find all levels in a list of factors

    Args:
        fs: A list of factors

    Returns:
        A list of all levels
    """
    raise _NotImplementedByCurrentBackendError("lvls_union", fs)
