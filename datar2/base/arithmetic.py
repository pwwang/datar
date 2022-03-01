"""Arithmetic or math functions"""

import math

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.core.groupby import SeriesGroupBy, GroupBy
from pandas.core.generic import NDFrame
from pipda import register_func, register_verb

from ..core.factory import func_factory
from ..core.tibble import Tibble, TibbleGrouped
from ..core.utils import (
    ensure_nparray,
    logger,
)
from ..core.contexts import Context

from .testing import is_numeric


def _check_all_numeric(x, fun_name):
    if x.apply(is_numeric).all():
        return

    raise ValueError(f"In {fun_name}(...): input must be all numeric.")


def _warn_na_rm(funcname, na_rm, extra_info=""):
    """Warn about na_rm False on SeriesGroupBy objects"""
    if not na_rm:
        logger.warning(
            "In %s(...): `na_rm` on GroupBy objects is always True. %s",
            funcname,
            extra_info,
        )


# cor?, range, summary, iqr
@func_factory("agg", stof=False)
def sum(x, na_rm=True):
    """Sum of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True, and you might want to use `f.x.sum(min_count=...)` to control
            NA produces
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`

    Returns:
        The sum of the input
    """
    func = np.nansum if na_rm else np.sum
    return func(x)


sum.register(
    (NDFrame, GroupBy),
    "sum",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "sum", na_rm, "Use f.x.sum(min_count=...) to control NA produces."
    )
    or (x, (), {}),
)


@func_factory("agg", stof=False)
def prod(x, na_rm=True):
    """Product of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True, and you might want to use `f.x.prod(min_count=...)` to control
            NA produces
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`

    Returns:
        The prod of the input
    """
    func = np.nanprod if na_rm else np.prod
    return func(x)


prod.register(
    (NDFrame, GroupBy),
    "prod",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "sum", na_rm, "Use f.x.prod(min_count=...) to control NA produces."
    )
    or (x, (), {}),
)


@func_factory("agg", stof=False)
def mean(x, na_rm=True):
    """Mean of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True.
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`

    Returns:
        The mean of the input
    """
    func = np.nanmean if na_rm else np.mean
    return func(x)


mean.register(
    (NDFrame, GroupBy),
    "mean",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "mean",
        na_rm,
    )
    or (x, (), {}),
)


@func_factory("agg", stof=False)
def median(x, na_rm=True):
    """Median of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True.
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`

    Returns:
        The median of the input
    """
    func = np.nanmedian if na_rm else np.median
    return func(x)


median.register(
    (NDFrame, GroupBy),
    "median",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "median",
        na_rm,
    )
    or (x, (), {}),
)


@func_factory("agg", stof=False)
def min(x, na_rm=True):
    """Min of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True, and you might want to use `f.x.min(min_count=...)` to control
            NA produces
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`

    Returns:
        The min of the input
    """
    func = np.nanmin if na_rm else np.min
    return func(x)


min.register(
    (NDFrame, GroupBy),
    "min",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "min", na_rm, "Use f.x.min(min_count=...) to control NA produces."
    )
    or (x, (), {}),
)


@func_factory("agg", stof=False)
def max(x, na_rm=True):
    """Max of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True, and you might want to use `f.x.max(min_count=...)` to control
            NA produces
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`

    Returns:
        The max of the input
    """
    func = np.nanmax if na_rm else np.max
    return func(x)


max.register(
    (NDFrame, GroupBy),
    "max",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "max", na_rm, "Use f.x.max(min_count=...) to control NA produces."
    )
    or (x, (), {}),
)


@func_factory("agg", stof=False)
def var(x, na_rm=True, ddof=1):
    """Variance of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs. If `x` is SeriesGroupBy object, this is always
            True
            And also unlike the function in `R`. It defaults to `True` rather
            than `False`
        ddof: Delta Degrees of Freedom

    Returns:
        The variance of the input
    """
    func = np.nanvar if na_rm else np.var
    return func(x, ddof=ddof)


var.register(
    (NDFrame, GroupBy),
    "var",
    pre=lambda x, na_rm=True, ddof=1: _warn_na_rm(
        "var",
        na_rm,
    )
    or (x, (), {"ddof": ddof}),
)


@register_func(None, context=Context.EVAL)
def pmin(*x, na_rm=False):
    """Get the min value rowwisely

    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise min of `*x`
    """
    return Tibble.from_args(*x).min(axis=1, skipna=na_rm)


@register_func(None, context=Context.EVAL)
def pmax(*x, na_rm=True):
    """Get the max value rowwisely
    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise max of `*x`
    """
    return Tibble.from_args(*x).max(axis=1, skipna=na_rm)


@func_factory("transform")
def round(x, ndigits=0):
    """Rounding a number

    Args:
        x: The input
        ndigits: number of digits to keep

    Returns:
        The rounded input
    """
    return np.round(x, decimals=ndigits)


round.register(
    (NDFrame, GroupBy),
    "round",
    pre=lambda x, ndigits=0: (x, (ndigits,), {})
)


sqrt = func_factory(
    "transform",
    doc="""Get the square root of a number/numbers

    Args:
        x: The input

    Returns:
        The square root of the input
    """,
    func=np.sqrt,
)


abs = func_factory(
    "transform",
    doc="""Get the absolute value of a number/numbers

    Args:
        x: The input

    Returns:
        The absolute values of the input
    """,
    func=np.abs,
)


sign = func_factory(
    "transform",
    doc="""Get the signs of the corresponding elements of x

    Args:
        x: The input

    Returns:
        The signs of the corresponding elements of x
    """,
    func=np.sign,
)


trunc = func_factory(
    "transform",
    doc="""Get the integers truncated for each element in x

    Args:
        x: The input

    Returns:
        The ingeters of elements in x being truncated
        Note the dtype is still float.
    """,
    func=np.trunc,
)


ceiling = func_factory(
    "transform",
    name="ceiling",
    doc="""Get the ceiling integer of a number/numbers

    Args:
        x: The input

    Returns:
        The ceiling integer of the input
    """,
    func=np.ceil,
)


floor = func_factory(
    "transform",
    doc="""Get the floor integer of a number/numbers

    Args:
        x: The input

    Returns:
        The floor integer of the input
    """,
    func=np.floor,
)


@func_factory("transform", is_vectorized=False, excluded={"digits"})
def signif(x: float, digits: int = 6) -> float:
    """Rounds the values in its first argument to the specified number of
    significant digits

    Args:
        x: A numeric vector or scalar
        digits: integer indicating the number of significant digits to be used

    Returns:
        The rounded values for each element in x
    """
    return round(x, int(digits - math.ceil(math.log10(abs(x)))))


@func_factory("transform")
def log(x, base=np.e):
    """Computes logarithms, by default natural logarithm

    Args:
        x: A numeric scalar or vector
        base: The base of the logarithm

    Returns:
        The value of the logarithm if x is scalar, otherwise element-wise
        logarithm of elements in x
    """
    return np.log(x) / np.log(base)


exp = func_factory(
    "transform",
    doc="""Calculates the power of natural number

    Args:
        x: A numeric scalar or vector

    Returns:
        Power of natural number of element-wise power of natural number for x
    """,
    func=np.exp,
)


log2 = func_factory(
    "transform",
    doc="""Computes logarithms with base 2

    Args:
        x: A numeric scalar or vector

    Returns:
        The value of log2 if x is scalar, otherwise element-wise
        log2 of elements in x
    """,
    func=np.log2,
)


log10 = func_factory(
    "transform",
    doc="""Computes logarithms with base 10

    Args:
        x: A numeric scalar or vector

    Returns:
        The value of log10 if x is scalar, otherwise element-wise
        log10 of elements in x
    """,
    func=np.log10,
)


log1p = func_factory(
    "transform",
    doc="""Computes log(1+x)

    Args:
        x: A numeric scalar or vector

    Returns:
        The value of log(1+x) if x is scalar, otherwise element-wise
        log(1+x) of elements in x
    """,
    func=np.log1p,
)


@register_verb(DataFrame, context=Context.EVAL)
def cov(x, y=None, ddof=1) -> Tibble:
    """Compute pairwise covariance of dataframe columns,
    or between two variables
    """
    if y is not None:
        raise ValueError(
            "In `cov(...)`: No `y` is allowed when `x` is a data frame."
        )
    if isinstance(x, TibbleGrouped):
        x = x._datar["grouped"]
        return x.cov(ddof=ddof).droplevel(-1)

    # support na_rm, use, method. see `?cov` in R?
    return x.cov(ddof=ddof)


@cov.register((list, tuple, np.ndarray, Series), context=Context.EVAL)
def _(x, y, ddof=1):
    """Compute covariance for two iterables"""
    # ddof: numpy v1.5+
    return np.cov(x, y, ddof=ddof)[0][1]


@cov.register(SeriesGroupBy, context=Context.EVAL)
def _(x, y, ddof=1):
    """Compute covariance for two iterables"""
    # ddof: numpy v1.5+
    from ..tibble import tibble

    df = tibble(x=x, cov=y)
    return df._datar["grouped"].cov(ddof=ddof).droplevel(-1)["cov"].iloc[::2]


@register_verb(DataFrame, context=Context.EVAL)
def _scale(x, center=True, scale=True):
    """Scaling and Centering of a numeric data frame

    See Details in `?scale` in `R`

    Args:
        x: The numeric data frame to scale
        center: either a logical value or numeric-alike vector of length
            equal to the number of columns of `x`
        scale: either a logical value or a numeric-alike vector of length
            equal to the number of columns of `x`.

    Returns:
        The centered, scaled data frame
    """
    _check_all_numeric(x, "scale")

    # center
    ncols = x.shape[1]
    center_is_true = center is True
    out_attrs = {}

    if center_is_true:
        center = x.mean(numeric_only=True)

    elif center is not False:
        center = ensure_nparray(center)
        if len(center) != ncols:
            raise ValueError(
                f"length of `center` ({len(center)}) must equal "
                f"the number of columns of `x` ({ncols})"
            )

    if center is not False:
        x = x.subtract(center)
        out_attrs["scaled:center"] = center

    # scale
    if scale is True:

        def _rms(col: Series) -> Series:
            nonnas = col[~pd.isnull(col)] ** 2
            return np.sqrt(nonnas.sum() / (len(nonnas) - 1))

        scale = x.std(numeric_only=True) if center_is_true else x.agg(_rms)

    elif scale is not False:
        scale = ensure_nparray(scale)
        if len(scale) != ncols:
            raise ValueError(
                f"length of `scale` ({len(center)}) must equal "
                f"the number of columns of `x` ({ncols})"
            )

    if scale is not False:
        x = x.div(scale)
        out_attrs["scaled:scale"] = scale

    if center is False and scale is False:
        x = x.copy()

    x.attrs.update(out_attrs)
    return x


@_scale.register(Series)
def _(x, center=True, scale=True):
    """Scaling on series"""
    return _scale(x.to_frame(), center, scale)


@_scale.register((list, tuple, np.ndarray))
def _(
    x,
    center=True,
    scale=True,
):
    """Scaling on iterables"""
    return _scale(Series(x, name="scaled"), center, scale)


scale = _scale


@register_verb(DataFrame)
def col_sums(
    x,
    na_rm=False,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate sum of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The sums by column.
    """
    _check_all_numeric(x, "col_sums")
    if isinstance(x, TibbleGrouped):
        _warn_na_rm("col_sums", na_rm)
        x = x._datar["grouped"]
        return x.sum(numeric_only=True)
    return x.sum(skipna=na_rm, numeric_only=True)


@register_verb(DataFrame)
def row_sums(
    x,
    na_rm=False,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate sum of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The sums by row.
    """
    _check_all_numeric(x, "row_sums")
    return x.sum(axis=1, skipna=na_rm, numeric_only=True)


@register_verb(DataFrame)
def col_means(
    x,
    na_rm=False,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate mean of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The means by column.
    """
    _check_all_numeric(x, "col_means")
    if isinstance(x, TibbleGrouped):
        _warn_na_rm("col_means", na_rm)
        x = x._datar["grouped"]
        return x.mean(numeric_only=True)
    return x.mean(skipna=na_rm, numeric_only=True)


@register_verb(DataFrame)
def row_means(
    x,
    na_rm=False,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate mean of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The means by row.
    """
    _check_all_numeric(x, "row_means")
    return x.mean(axis=1, skipna=na_rm, numeric_only=True)


@register_verb(DataFrame)
def col_sds(
    x,
    na_rm=False,
    ddof=1,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate stdev of a data frame by column

    Args:
        x: The data frame
        ddof: Delta Degrees of Freedom.
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The stdevs by column.
    """
    _check_all_numeric(x, "col_sds")
    if isinstance(x, TibbleGrouped):
        _warn_na_rm("col_sds", na_rm)
        x = x._datar["grouped"]
        return x.std(ddof=ddof)
    return x.std(skipna=na_rm, ddof=ddof, numeric_only=True)


@register_verb(DataFrame)
def row_sds(
    x,
    na_rm=False,
    ddof=1,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate stdev of a data frame by row

    Args:
        x: The data frame
        ddof: Delta Degrees of Freedom.
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The stdevs by row.
    """
    return x.std(axis=1, skipna=na_rm, ddof=ddof, numeric_only=True)


@register_verb(DataFrame)
def col_medians(
    x,
    na_rm=False,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate median of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The medians by column.
    """
    _check_all_numeric(x, "col_medians")
    if isinstance(x, TibbleGrouped):
        _warn_na_rm("col_medians", na_rm)
        x = x._datar["grouped"]
        return x.median(numeric_only=True)
    return x.median(skipna=na_rm, numeric_only=True)


@register_verb(DataFrame)
def row_medians(
    x,
    na_rm=False,
    # dims=1,
    # weights = None,
    # freq = None,
    # n = None
):
    """Calculate median of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The medians by row.
    """
    _check_all_numeric(x, "row_medians")
    return x.median(numeric_only=True, axis=1, skipna=na_rm)
