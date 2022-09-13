"""Arithmetic or math functions"""

import inspect
from typing import TYPE_CHECKING, Union

import numpy as np
from pipda import register_verb
from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.groupby import SeriesGroupBy, GroupBy

from ..core.factory import func_factory
from ..core.tibble import Tibble, TibbleGrouped, TibbleRowwise
from ..core.utils import ensure_nparray, logger
from ..core.contexts import Context

if TYPE_CHECKING:
    from pandas.core.generic import NDFrame

SINGLE_ARG_SIGNATURE = inspect.signature(lambda x: None)


def _check_all_numeric(x, fun_name):
    from .testing import is_numeric

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
@func_factory(kind="agg")
def sum(x: "NDFrame", na_rm: bool = True) -> "NDFrame":
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
    return x.sum(skipna=na_rm)


sum.register(
    (TibbleGrouped, SeriesGroupBy),
    func="sum",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "sum", na_rm, "Use f.x.sum(min_count=...) to control NA produces."
    ) or (x, (), {}),
)


@func_factory(kind="agg")
def prod(x: "NDFrame", na_rm: bool = True) -> "NDFrame":
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
    return x.prod(skipna=na_rm)


prod.register(
    (TibbleGrouped, GroupBy),
    func="prod",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "prod", na_rm, "Use f.x.prod(min_count=...) to control NA produces."
    )
    or (x, (), {}),
)


@func_factory(kind="agg")
def mean(x: "NDFrame", na_rm=True) -> "NDFrame":
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
    return x.mean(skipna=na_rm)


mean.register(
    (TibbleGrouped, SeriesGroupBy, TibbleRowwise),
    func="mean",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "mean",
        na_rm,
    ) or (x, (), {}),
)


@func_factory(kind="agg")
def median(x: "NDFrame", na_rm: bool = True) -> "NDFrame":
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
    return x.median(skipna=na_rm)


median.register(
    (TibbleGrouped, GroupBy),
    func="median",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "median",
        na_rm,
    ) or (x, (), {}),
)


@func_factory(kind="agg")
def min(x: "NDFrame", na_rm: bool = True) -> "NDFrame":
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
    return x.min(skipna=na_rm)


min.register(
    (TibbleGrouped, GroupBy),
    func="min",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "min", na_rm, "Use f.x.min(min_count=...) to control NA produces."
    ) or (x, (), {}),
)


@func_factory(kind="agg")
def max(x: "NDFrame", na_rm: bool = True) -> "NDFrame":
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
    return x.max(skipna=na_rm)


max.register(
    (TibbleGrouped, GroupBy),
    func="max",
    pre=lambda x, na_rm=True: _warn_na_rm(
        "max", na_rm, "Use f.x.max(min_count=...) to control NA produces."
    ) or (x, (), {}),
)


@func_factory(kind="agg")
def var(x: "NDFrame", na_rm: bool = True, ddof: int = 1) -> "NDFrame":
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
    return x.var(skipna=na_rm, ddof=ddof)


var.register(
    (TibbleGrouped, GroupBy),
    func="var",
    pre=lambda x, na_rm=True, ddof=1: _warn_na_rm(
        "var",
        na_rm,
    )
    or (x, (), {"ddof": ddof}),
)


@func_factory(kind="agg")
def pmin(
    *x: Union["NDFrame", GroupBy],
    na_rm: bool = False,
    __args_frame: DataFrame = None,
) -> Series:
    """Get the min value rowwisely

    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise min of `*x`
    """
    return __args_frame.min(axis=1, skipna=na_rm)


@func_factory(kind="agg")
def pmax(
    *x: Union["NDFrame", GroupBy],
    na_rm: bool = False,
    __args_frame: DataFrame = None,
) -> Series:
    """Get the max value rowwisely
    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise max of `*x`
    """
    return __args_frame.max(axis=1, skipna=na_rm)


round = func_factory(
    kind="transform",
    doc="""Rounding a number

    Args:
        x: The input
        ndigits: number of digits to keep. Must be positional argument.

    Returns:
        The rounded input
    """,
    signature=inspect.signature(lambda x, ndigits=0: None),
    func=np.round,
)


sqrt = func_factory(
    kind="transform",
    doc="""Get the square root of a number/numbers

    Args:
        x: The input

    Returns:
        The square root of the input
    """,
    qualname="sqrt",
    module="datar.base",
    func=np.sqrt,
    signature=SINGLE_ARG_SIGNATURE,
)


abs = func_factory(
    kind="transform",
    doc="""Get the absolute value of a number/numbers

    Args:
        x: The input

    Returns:
        The absolute values of the input
    """,
    func=np.abs,
    qualname="abs",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


sign = func_factory(
    kind="transform",
    doc="""Get the signs of the corresponding elements of x

    Args:
        x: The input

    Returns:
        The signs of the corresponding elements of x
    """,
    func=np.sign,
    qualname="sign",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


trunc = func_factory(
    kind="transform",
    doc="""Get the integers truncated for each element in x

    Args:
        x: The input

    Returns:
        The ingeters of elements in x being truncated
        Note the dtype is still float.
    """,
    func=np.trunc,
    qualname="trunc",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


ceiling = func_factory(
    kind="transform",
    doc="""Get the ceiling integer of a number/numbers

    Args:
        x: The input

    Returns:
        The ceiling integer of the input
    """,
    func=np.ceil,
    name="ceiling",
    qualname="ceiling",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


floor = func_factory(
    kind="transform",
    doc="""Get the floor integer of a number/numbers

    Args:
        x: The input

    Returns:
        The floor integer of the input
    """,
    func=np.floor,
    qualname="floor",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


@func_factory({"x", "digits"}, "transform")
def signif(x: Series, digits: Series = 6) -> Series:
    """Rounds the values in its first argument to the specified number of
    significant digits

    Args:
        x: A numeric vector or scalar
        digits: integer indicating the number of significant digits to be used

    Returns:
        The rounded values for each element in x
    """
    ndigits = digits - x.abs().transform("log10").transform("ceil")
    return Series(
        np.vectorize(np.round)(x, ndigits.astype(int)), index=x.index
    )


@func_factory({"x", "base"}, "transform")
def log(x: Series, base: Series = np.e) -> Series:
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
    kind="transform",
    doc="""Calculates the power of natural number

    Args:
        x: A numeric scalar or vector

    Returns:
        Power of natural number of element-wise power of natural number for x
    """,
    func=np.exp,
    qualname="exp",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


log2 = func_factory(
    kind="transform",
    doc="""Computes logarithms with base 2

    Args:
        x: A numeric scalar or vector

    Returns:
        The value of log2 if x is scalar, otherwise element-wise
        log2 of elements in x
    """,
    func=np.log2,
    qualname="log2",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


log10 = func_factory(
    kind="transform",
    doc="""Computes logarithms with base 10

    Args:
        x: A numeric scalar or vector

    Returns:
        The value of log10 if x is scalar, otherwise element-wise
        log10 of elements in x
    """,
    func=np.log10,
    qualname="log10",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
)


log1p = func_factory(
    kind="transform",
    doc="""Computes log(1+x)

    Args:
        x: A numeric scalar or vector

    Returns:
        The value of log(1+x) if x is scalar, otherwise element-wise
        log(1+x) of elements in x
    """,
    func=np.log1p,
    qualname="log1p",
    module="datar.base",
    signature=SINGLE_ARG_SIGNATURE,
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
    return _scale(x.to_frame(), center, scale).iloc[:, 0]


@_scale.register(SeriesGroupBy)
def _(x, center=True, scale=True):
    """Scaling on series"""
    return x.transform(
        _scale.dispatch(Series),
        center=center,
        scale=scale,
    ).groupby(
        x.grouper,
        sort=x.sort,
        dropna=x.dropna,
        observed=x.observed,
    )


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


@func_factory(kind="agg")
def quantile(
    x: Series,
    probs=(0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = True,
    interpolation: str = "linear",  # Use method for numpy 1.22+
):
    """produces sample quantiles corresponding to the given probabilities.

    Args:
        x: The data to sample
        probs: numeric vector of probabilities with values in [0,1]
        na_rm: if true, any ‘NA’ and ‘NaN’'s are removed from ‘x’
            before the quantiles are computed.
        quantile: {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
            This optional parameter specifies the interpolation method to use,
            when the desired quantile lies between two data points i and j.
            fractional part of the index surrounded by i and j.
            - lower: i.
            - higher: j.
            - nearest: i or j whichever is nearest.
            - midpoint: (i + j) / 2.

    Returns:
        An array of quantile values
    """
    _warn_na_rm("quantile", na_rm)
    return x.quantile(q=probs, interpolation=interpolation)


@quantile.register_dispatchee(SeriesGroupBy)
def _(
    x: Series,
    probs=(0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = True,
    interpolation: str = "linear",  # Use method for numpy 1.22+
):
    _warn_na_rm("quantile", na_rm)
    out = x.quantile(q=probs, interpolation=interpolation)
    if not is_scalar(probs):
        out = out.droplevel(-1)

    return out


@func_factory(kind="agg")
def std(
    x: Series,
    na_rm: bool = True,
    # numpy default is 0. Make it 1 to be consistent with R
    ddof: int = 1,
) -> float:
    """Get standard deviation of the input"""
    return x.std(skipna=na_rm, ddof=ddof)


std.register(
    (TibbleGrouped, GroupBy),
    func="std",
    pre=lambda x, na_rm=True, ddof=1: _warn_na_rm("sd/std", na_rm)
    or (x, (), {"ddof": ddof}),
)

sd = std


@func_factory({"x", "w"})
def weighted_mean(
    x: Series, w: Series = 1, na_rm=True, __args_raw=None
) -> Series:
    """Calculate weighted mean"""
    if __args_raw["w"] is None:
        return np.nanmean(x) if na_rm else np.mean(x)

    if np.nansum(w) == 0:
        return np.nan

    if na_rm:
        na_mask = pd.isnull(x)
        x = x[~na_mask.values]
        w = w[~na_mask.values]
        if w.size == 0:
            return np.nan
        return np.average(x, weights=w)

    return np.average(x, weights=w)
