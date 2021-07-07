"""Arithmetic or math functions"""

from typing import Any, Callable, Iterable, Union

import numpy
from pandas import DataFrame, Series
from pipda import register_func, register_verb

from ..core.contexts import Context
from ..core.types import NumericOrIter, NumericType, is_not_null, is_scalar
from ..core.utils import Array, register_numpy_func_x, recycle_value, length_of
from ..core.collections import Collection

# cor?, range, summary, iqr


def _register_arithmetic_agg(
    name: str, np_name: str, doc: str = ""
) -> Callable:
    """Register an arithmetic function"""

    @register_func(None, context=Context.EVAL)
    def _arithmetric(x: Iterable, na_rm: bool = False) -> Iterable:
        """Arithmetric function"""
        # na_rm not working for numpy functions
        # with x is a Series object
        if isinstance(x, Series):
            return getattr(x, np_name)(skipna=na_rm)

        fun_name = f"nan{np_name}" if na_rm else np_name
        return getattr(numpy, fun_name)(x)

    _arithmetric.__name__ = name
    _arithmetric.__doc__ = doc
    return _arithmetric


# pylint: disable=invalid-name
# pylint: disable=redefined-builtin
sum = _register_arithmetic_agg(
    "sum",
    "sum",
    doc="""Sum of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The sum of the input
    """,
)

mean = _register_arithmetic_agg(
    "mean",
    "mean",
    doc="""Mean of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The mean of the input
    """,
)

median = _register_arithmetic_agg(
    "median",
    "median",
    doc="""Median of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The median of the input
    """,
)


@register_func(None, context=Context.EVAL)
def min(*x, na_rm: bool = False) -> Any:
    """Min of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The min of the input
    """
    fun = numpy.nanmin if na_rm else numpy.min
    x = Collection(*x)  # flatten
    return fun(x)


@register_func(None, context=Context.EVAL)
def max(*x, na_rm: bool = False) -> Any:
    """Max of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs

    Returns:
        The max of the input
    """
    fun = numpy.nanmax if na_rm else numpy.max
    x = Collection(*x)  # flatten
    return fun(x)


@register_func(None, context=Context.EVAL)
def var(x: Any, na_rm: bool = False, ddof: int = 1):
    """Variance of the input.

    Args:
        x: The input
        na_rm: Exclude the NAs
        ddof: Delta Degrees of Freedom

    Returns:
        The variance of the input
    """
    fun = numpy.nanvar if na_rm else numpy.var
    return fun(x, ddof=ddof)


@register_func(None, context=Context.EVAL)
def pmin(*x: NumericType, na_rm: bool = False) -> Iterable[float]:
    """Get the min value rowwisely

    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise min of `*x`
    """
    maxlen = max(map(length_of, x))
    x = (recycle_value(elem, maxlen) for elem in x)
    return Array([min(elem, na_rm=na_rm) for elem in zip(*x)])


@register_func(None, context=Context.EVAL)
def pmax(*x: Iterable, na_rm: bool = False) -> Iterable[float]:
    """Get the max value rowwisely
    Args:
        *x: The iterables. Elements will be recycled to the max length
        na_rm: Whether ignore the NAs

    Returns:
        The rowwise max of `*x`
    """
    maxlen = max(map(length_of, x))
    x = (recycle_value(elem, maxlen) for elem in x)
    return Array([max(elem, na_rm=na_rm) for elem in zip(*x)])


@register_func(None, context=Context.EVAL)
def round(x: NumericOrIter, ndigits: int = 0) -> NumericOrIter:
    """Rounding a number"""
    return numpy.round(x, ndigits)


sqrt = register_numpy_func_x(
    "sqrt",
    "sqrt",
    doc="""Get the square root of a number/numbers

    Args:
        x: The input

    Returns:
        The square root of the input
    """,
)

abs = register_numpy_func_x(
    "abs",
    "abs",
    doc="""Get the absolute value of a number/numbers

    Args:
        x: The input

    Returns:
        The absolute values of the input
    """,
)

ceiling = register_numpy_func_x(
    "ceiling",
    "ceil",
    doc="""Get the ceiling integer of a number/numbers

    Args:
        x: The input

    Returns:
        The ceiling integer of the input
    """,
)

floor = register_numpy_func_x(
    "floor",
    "floor",
    doc="""Get the floor integer of a number/numbers

    Args:
        x: The input

    Returns:
        The floor integer of the input
    """,
)

# pylint: disable=unused-argument
@register_verb(DataFrame, context=Context.EVAL)
def cov(x: DataFrame, y: Iterable = None, ddof: int = 1) -> DataFrame:
    """Compute pairwise covariance of dataframe columns,
    or between two variables
    """
    # TODO: support na_rm, use, method. see `?cov` in R
    return x.cov(ddof=ddof)


@cov.register((numpy.ndarray, Series, list, tuple), context=Context.EVAL)
def _(x: Iterable, y: Iterable, ddof: int = 1) -> DataFrame:
    """Compute covariance for two iterables"""
    # ddof: numpy v1.5+
    return numpy.cov(x, y, ddof=ddof)[0][1]


@register_verb(DataFrame, context=Context.EVAL)
def scale(
    x: DataFrame,
    center: Union[bool, NumericOrIter] = True,
    # pylint: disable=redefined-outer-name
    scale: Union[bool, NumericOrIter] = True,
) -> DataFrame:
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
    # center
    ncols = x.shape[1]
    center_is_true = center is True
    out_attrs = {}

    if center is True:
        center = col_means(x)

    elif center is not False:
        if is_scalar(center):
            center = [center] # type: ignore
        if len(center) != ncols:
            raise ValueError(
                f"length of `center` ({len(center)}) must equal "
                f"the number of columns of `x` ({ncols})"
            )

    if center is not False:
        x = x - center
        out_attrs["scaled:center"] = Array(center)

    # scale
    if scale is True:

        def _rms(col: Series) -> Series:
            nonnas = col[is_not_null(col)] ** 2
            return sqrt(nonnas.sum() / (len(nonnas) - 1))

        scale = col_sds(x) if center_is_true else x.agg(_rms)

    elif scale is not False:
        if is_scalar(scale):
            scale = [scale] # type: ignore
        if len(scale) != ncols:
            raise ValueError(
                f"length of `scale` ({len(center)}) must equal "
                f"the number of columns of `x` ({ncols})"
            )

    if scale is not False:
        x = x / scale
        out_attrs["scaled:scale"] = Array(scale)

    if center is False and scale is False:
        x = x.copy()

    x.attrs.update(out_attrs)
    return x


# being able to refer it inside the function
# as scale also used as an argument
_scale = scale


@scale.register(Series)
def _(
    x: Series,
    center: Union[bool, NumericOrIter] = True,
    # pylint: disable=redefined-outer-name
    scale: Union[bool, NumericOrIter] = True,
) -> DataFrame:
    """Scaling on series"""
    return _scale(x.to_frame(), center, scale)


@scale.register((numpy.ndarray, list, tuple))
def _(
    x: Iterable,
    center: Union[bool, NumericOrIter] = True,
    # pylint: disable=redefined-outer-name
    scale: Union[bool, NumericOrIter] = True,
) -> DataFrame:
    """Scaling on iterables"""
    return _scale(Series(x, name="scaled"), center, scale)


@register_verb(DataFrame)
def col_sums(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate sum of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The sums by column.
    """
    return x.agg(sum, na_rm=na_rm)


@register_verb(DataFrame)
def row_sums(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate sum of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The sums by row.
    """
    return x.agg(sum, axis=1, na_rm=na_rm)


@register_verb(DataFrame)
def col_means(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate mean of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The means by column.
    """
    return x.agg(mean, na_rm=na_rm)


@register_verb(DataFrame)
def row_means(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate mean of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The means by row.
    """
    return x.agg(mean, axis=1, na_rm=na_rm)


@register_verb(DataFrame)
def col_sds(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate stdev of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The stdevs by column.
    """
    from ..stats import sd

    return x.agg(sd, na_rm=na_rm)


@register_verb(DataFrame)
def row_sds(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate stdev of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The stdevs by row.
    """
    from ..stats import sd

    return x.agg(sd, axis=1, na_rm=na_rm)


@register_verb(DataFrame)
def col_medians(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate median of a data frame by column

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The medians by column.
    """
    return x.agg(median, na_rm=na_rm)


@register_verb(DataFrame)
def row_medians(
    x: DataFrame,
    na_rm: bool = False,
    # dims: int = 1,
    # weights = None,
    # freq = None,
    # n = None
) -> Iterable[NumericType]:
    """Calculate median of a data frame by row

    Args:
        x: The data frame
        na_rm: Specifies how to handle missing values in `x`.

    Returns:
        The medians by row.
    """
    return x.agg(median, axis=1, na_rm=na_rm)
