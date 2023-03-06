"""APIs ported from r-base"""
# import the variables with _ so that they are not imported by *
import math as _math
from typing import Any
from string import ascii_letters as _ascii_letters

from pipda import register_func as _register_func

from ..core.utils import (
    NotImplementedByCurrentBackendError as _NotImplementedByCurrentBackendError,
    CollectionFunction as _CollectionFunction,
)
from ..core.options import options, get_option, options_context  # noqa: F401
from ..core.names import repair_names as _repair_names

pi = _math.pi
letters = list(_ascii_letters[:26])
LETTERS = list(_ascii_letters[26:])
month_name = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
month_abb = [m[:3] for m in month_name]

FALSE = False
TRUE = True
NA = float("nan")
NULL = None
NaN = float("nan")
Inf = float("inf")


@_register_func(pipeable=True, dispatchable=True)
def ceiling(x) -> Any:
    """Round up to the nearest integer

    Args:
        x: The value to be rounded up

    Returns:
        The rounded up value
    """
    raise _NotImplementedByCurrentBackendError("ceiling", x)


@_register_func(pipeable=True, dispatchable=True)
def cov(x, y=None, na_rm: bool = False, ddof: int = 1) -> Any:
    """Compute pairwise covariance between two variables

    Args:
        x: a numeric vector, matrix or data frame.
        y: None or a vector, matrix or data frame with
          compatible dimensions to `x`.  The default is equivalent to
          `y = x`
        na_rm: If `True`, remove missing values before computing
            the covariance.
        ddof: The denominator degrees of freedom.

    Returns:
        The covariance matrix
    """
    raise _NotImplementedByCurrentBackendError("cov", x)


@_register_func(pipeable=True, dispatchable=True)
def floor(x) -> Any:
    """Round down to the nearest integer

    Args:
        x: The value to be rounded down

    Returns:
        The rounded down value
    """
    raise _NotImplementedByCurrentBackendError("floor", x)


@_register_func(pipeable=True, dispatchable=True)
def mean(x, na_rm: bool = False) -> Any:
    """Compute the mean of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The mean of the vector
    """
    raise _NotImplementedByCurrentBackendError("mean", x)


@_register_func(pipeable=True, dispatchable=True)
def median(x, na_rm: bool = False) -> Any:
    """Compute the median of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The median of the vector
    """
    raise _NotImplementedByCurrentBackendError("median", x)


@_register_func(pipeable=True, dispatchable=True)
def pmax(*args, na_rm: bool = False) -> Any:
    """Returns the (regular or Parallel) maxima and minima of the input
     values.

    Args:
        x: A numeric vector
        more: One or more values
        na_rm: Whether to remove `NA` values

    Returns:
        The maximum of the vector and the values
    """
    raise _NotImplementedByCurrentBackendError("pmax")


@_register_func(pipeable=True, dispatchable=True)
def pmin(*args, na_rm: bool = False) -> Any:
    """Returns the (regular or Parallel) maxima and minima of the input
     values.

    Args:
        x: A numeric vector
        more: One or more values
        na_rm: Whether to remove `NA` values

    Returns:
        The minimum of the vector and the values
    """
    raise _NotImplementedByCurrentBackendError("pmin")


@_register_func(pipeable=True, dispatchable=True)
def sqrt(x) -> Any:
    """Compute the square root of a vector

    Args:
        x: A numeric vector

    Returns:
        The square root of the vector
    """
    raise _NotImplementedByCurrentBackendError("sqrt", x)


@_register_func(pipeable=True, dispatchable=True)
def var(x, na_rm: bool = False, ddof: int = 1) -> Any:
    """Compute the variance of a vector

    Args:
        x: A numeric vector
        y: None or a vector, matrix or data frame with
          compatible dimensions to `x`.  The default is equivalent to
          `y = x`
        na_rm: Whether to remove `NA` values
        ddof: The degrees of freedom

    Returns:
        The variance of the vector
    """
    raise _NotImplementedByCurrentBackendError("var", x)


@_register_func(pipeable=True, dispatchable=True)
def scale(x, center=True, scale_=True) -> Any:
    """Center and/or scale the data

    Args:
        x: A numeric vector
        center: Whether to center the data
        scale_: Whether to scale the data

    Returns:
        The scaled data
    """
    raise _NotImplementedByCurrentBackendError("scale", x)


@_register_func(pipeable=True, dispatchable=True)
def col_sums(x, na_rm: bool = False) -> Any:
    """Compute the column sums of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The column sums of the matrix
    """
    raise _NotImplementedByCurrentBackendError("col_sums", x)


@_register_func(pipeable=True, dispatchable=True)
def col_means(x, na_rm: bool = False) -> Any:
    """Compute the column means of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The column means of the matrix
    """
    raise _NotImplementedByCurrentBackendError("col_means", x)


@_register_func(pipeable=True, dispatchable=True)
def col_sds(x, na_rm: bool = False) -> Any:
    """Compute the column standard deviations of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The column standard deviations of the matrix
    """
    raise _NotImplementedByCurrentBackendError("col_sds", x)


@_register_func(pipeable=True, dispatchable=True)
def col_medians(x, na_rm: bool = False) -> Any:
    """Compute the column medians of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The column medians of the matrix
    """
    raise _NotImplementedByCurrentBackendError("col_medians", x)


@_register_func(pipeable=True, dispatchable=True)
def row_sums(x, na_rm: bool = False) -> Any:
    """Compute the row sums of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The row sums of the matrix
    """
    raise _NotImplementedByCurrentBackendError("row_sums", x)


@_register_func(pipeable=True, dispatchable=True)
def row_means(x, na_rm: bool = False) -> Any:
    """Compute the row means of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The row means of the matrix
    """
    raise _NotImplementedByCurrentBackendError("row_means", x)


@_register_func(pipeable=True, dispatchable=True)
def row_sds(x, na_rm: bool = False) -> Any:
    """Compute the row standard deviations of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The row standard deviations of the matrix
    """
    raise _NotImplementedByCurrentBackendError("row_sds", x)


@_register_func(pipeable=True, dispatchable=True)
def row_medians(x, na_rm: bool = False) -> Any:
    """Compute the row medians of a matrix

    Args:
        x: A numeric matrix
        na_rm: Whether to remove `NA` values

    Returns:
        The row medians of the matrix
    """
    raise _NotImplementedByCurrentBackendError("row_medians", x)


@_register_func(pipeable=True, dispatchable=True)
def min_(x, na_rm: bool = False) -> Any:
    """Compute the minimum of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The minimum of the vector
    """
    raise _NotImplementedByCurrentBackendError("min", x)


@_register_func(pipeable=True, dispatchable=True)
def max_(x, na_rm: bool = False) -> Any:
    """Compute the maximum of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The maximum of the vector
    """
    raise _NotImplementedByCurrentBackendError("max", x)


@_register_func(pipeable=True, dispatchable=True)
def round_(x, digits: int = 0) -> Any:
    """Round the values of a vector

    Args:
        x: A numeric vector
        digits: The number of digits to round to

    Returns:
        The rounded values
    """
    raise _NotImplementedByCurrentBackendError("round", x)


@_register_func(pipeable=True, dispatchable=True)
def sum_(x, na_rm: bool = False) -> Any:
    """Compute the sum of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The sum of the vector
    """
    raise _NotImplementedByCurrentBackendError("sum", x)


@_register_func(pipeable=True, dispatchable=True)
def abs_(x) -> Any:
    """Compute the absolute value of a vector

    Args:
        x: A numeric vector

    Returns:
        The absolute values of the vector
    """
    raise _NotImplementedByCurrentBackendError("abs", x)


@_register_func(pipeable=True, dispatchable=True)
def prod(x, na_rm: bool = False) -> Any:
    """Compute the product of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The product of the vector
    """
    raise _NotImplementedByCurrentBackendError("prod", x)


@_register_func(pipeable=True, dispatchable=True)
def sign(x) -> Any:
    """Compute the sign of a vector

    Args:
        x: A numeric vector

    Returns:
        The signs of the vector
    """
    raise _NotImplementedByCurrentBackendError("sign", x)


@_register_func(pipeable=True, dispatchable=True)
def signif(x, digits: int = 6) -> Any:
    """Round the values of a vector to a given number of significant digits

    Args:
        x: A numeric vector
        digits: The number of significant digits to round to

    Returns:
        The rounded values
    """
    raise _NotImplementedByCurrentBackendError("signif", x)


@_register_func(pipeable=True, dispatchable=True)
def trunc(x) -> Any:
    """Truncate the values of a vector

    Args:
        x: A numeric vector

    Returns:
        The truncated values
    """
    raise _NotImplementedByCurrentBackendError("trunc", x)


@_register_func(pipeable=True, dispatchable=True)
def exp(x) -> Any:
    """Compute the exponential of a vector

    Args:
        x: A numeric vector

    Returns:
        The exponential values
    """
    raise _NotImplementedByCurrentBackendError("exp", x)


@_register_func(pipeable=True, dispatchable=True)
def log(x, base: float = _math.e) -> Any:
    """Compute the logarithm of a vector

    Args:
        x: A numeric vector
        base: The base of the logarithm

    Returns:
        The logarithm values
    """
    raise _NotImplementedByCurrentBackendError("log", x)


@_register_func(pipeable=True, dispatchable=True)
def log2(x) -> Any:
    """Compute the base-2 logarithm of a vector

    Args:
        x: A numeric vector

    Returns:
        The logarithm values
    """
    raise _NotImplementedByCurrentBackendError("log2", x)


@_register_func(pipeable=True, dispatchable=True)
def log10(x) -> Any:
    """Compute the base 10 logarithm of a vector

    Args:
        x: A numeric vector

    Returns:
        The logarithm values
    """
    raise _NotImplementedByCurrentBackendError("log10", x)


@_register_func(pipeable=True, dispatchable=True)
def log1p(x) -> Any:
    """Compute the logarithm of one plus a vector

    Args:
        x: A numeric vector

    Returns:
        The logarithm values
    """
    raise _NotImplementedByCurrentBackendError("log1p", x)


@_register_func(pipeable=True, dispatchable=True)
def sd(x, na_rm: bool = False) -> Any:
    """Compute the standard deviation of a vector

    Args:
        x: A numeric vector
        na_rm: Whether to remove `NA` values

    Returns:
        The standard deviation of the vector
    """
    raise _NotImplementedByCurrentBackendError("sd", x)


@_register_func(pipeable=True, dispatchable=True)
def weighted_mean(x, w=None, na_rm: bool = False) -> Any:
    """Compute the weighted mean of a vector

    Args:
        x: A numeric vector
        w: The weights to use
        na_rm: Whether to remove `NA` values

    Returns:
        The weighted mean of the vector
    """
    raise _NotImplementedByCurrentBackendError("weighted_mean", x)


@_register_func(pipeable=True, dispatchable=True)
def quantile(
    x,
    probs=(0.0, 0.25, 0.5, 0.75, 1.0),
    na_rm: bool = False,
    names: bool = True,
    type_: int = 7,
    digits: int = 7,
) -> Any:
    """Compute the quantiles of a vector

    Args:
        x: A numeric vector
        probs: The probabilities to use

    Returns:
        The quantiles of the vector
    """
    raise _NotImplementedByCurrentBackendError("quantile", x)


@_register_func(pipeable=True, dispatchable=True)
def bessel_i(x, nu, expon_scaled: bool = False) -> Any:
    """Compute the modified Bessel function of the first kind

    Args:
        x: A numeric vector
        nu: The order of the Bessel function
        expon_scaled: Whether to use the scaled version

    Returns:
        The Bessel function values
    """
    raise _NotImplementedByCurrentBackendError("bessel_i", x)


@_register_func(pipeable=True, dispatchable=True)
def bessel_j(x, nu) -> Any:
    """Compute the Bessel function of the first kind

    Args:
        x: A numeric vector
        nu: The order of the Bessel function

    Returns:
        The Bessel function values
    """
    raise _NotImplementedByCurrentBackendError("bessel_j", x)


@_register_func(pipeable=True, dispatchable=True)
def bessel_k(x, nu, expon_scaled: bool = False) -> Any:
    """Compute the modified Bessel function of the second kind

    Args:
        x: A numeric vector
        nu: The order of the Bessel function
        expon_scaled: Whether to use the scaled version

    Returns:
        The Bessel function values
    """
    raise _NotImplementedByCurrentBackendError("bessel_k", x)


@_register_func(pipeable=True, dispatchable=True)
def bessel_y(x, nu) -> Any:
    """Compute the Bessel function of the second kind

    Args:
        x: A numeric vector
        nu: The order of the Bessel function

    Returns:
        The Bessel function values
    """
    raise _NotImplementedByCurrentBackendError("bessel_y", x)


@_register_func(pipeable=True, dispatchable=True)
def as_double(x) -> Any:
    """Convert a vector to a double vector

    Args:
        x: A numeric vector

    Returns:
        The double vector
    """
    raise _NotImplementedByCurrentBackendError("as_double", x)


@_register_func(pipeable=True, dispatchable=True)
def as_integer(x) -> Any:
    """Convert a vector to an integer vector

    Args:
        x: A numeric vector

    Returns:
        The integer vector
    """
    raise _NotImplementedByCurrentBackendError("as_integer", x)


@_register_func(pipeable=True, dispatchable=True)
def as_logical(x) -> Any:
    """Convert a vector to a logical vector

    Args:
        x: A numeric vector

    Returns:
        The logical vector
    """
    raise _NotImplementedByCurrentBackendError("as_logical", x)


@_register_func(pipeable=True, dispatchable=True)
def as_character(x) -> Any:
    """Convert a vector to a character vector

    Args:
        x: A numeric vector

    Returns:
        The character vector
    """
    raise _NotImplementedByCurrentBackendError("as_character", x)


@_register_func(pipeable=True, dispatchable=True)
def as_factor(x) -> Any:
    """Convert a vector to a factor vector

    Args:
        x: A numeric vector

    Returns:
        The factor vector
    """
    raise _NotImplementedByCurrentBackendError("as_factor", x)


@_register_func(pipeable=True, dispatchable=True)
def as_ordered(x) -> Any:
    """Convert a vector to an ordered vector

    Args:
        x: A numeric vector

    Returns:
        The ordered vector
    """
    raise _NotImplementedByCurrentBackendError("as_ordered", x)


@_register_func(pipeable=True, dispatchable=True)
def as_date(
    x,
    *,
    format=None,
    try_formats=None,
    optional=False,
    tz=0,
    origin=None,
) -> Any:
    """Convert an object to a datetime.date object

    See: https://rdrr.io/r/base/as.Date.html

    Args:
        x: Object that can be converted into a datetime.date object
        format:  If not specified, it will try try_formats one by one on
            the first non-np.nan element, and give an error if none works.
            Otherwise, the processing is via strptime
        try_formats: vector of format strings to try if format is not specified.
            Default formats to try:
            "%Y-%m-%d"
            "%Y/%m/%d"
            "%Y-%m-%d %H:%M:%S"
            "%Y/%m/%d %H:%M:%S"
        optional: indicating to return np.nan (instead of signalling an error)
            if the format guessing does not succeed.
        origin: a datetime.date/datetime object, or something which can be
            coerced by as_date(origin, ...) to such an object.
        tz: a time zone offset or a datetime.timedelta object.
            Note that time zone name is not supported yet.

    Returns:
        The datetime.date object
    """
    raise _NotImplementedByCurrentBackendError("as_date", x)


@_register_func(pipeable=True, dispatchable=True)
def as_numeric(x) -> Any:
    """Convert a vector to a numeric vector

    Args:
        x: A numeric vector

    Returns:
        The numeric vector
    """
    raise _NotImplementedByCurrentBackendError("as_numeric", x)


@_register_func(pipeable=True, dispatchable=True)
def arg(x) -> Any:
    """Angles of complex numbers

    Args:
        x: A numeric vector

    Returns:
        The angles
    """
    raise _NotImplementedByCurrentBackendError("arg", x)


@_register_func(pipeable=True, dispatchable=True)
def conj(x) -> Any:
    """Complex conjugate

    Args:
        x: A numeric vector

    Returns:
        The complex conjugates
    """
    raise _NotImplementedByCurrentBackendError("conj", x)


@_register_func(pipeable=True, dispatchable=True)
def mod(x) -> Any:
    """Modulus of complex numbers

    Args:
        x: A numeric vector

    Returns:
        The modulus
    """
    raise _NotImplementedByCurrentBackendError("mod", x)


@_register_func(pipeable=True, dispatchable=True)
def re_(x) -> Any:
    """Real part of complex numbers

    Args:
        x: A numeric vector

    Returns:
        The real parts
    """
    raise _NotImplementedByCurrentBackendError("re", x)


@_register_func(pipeable=True, dispatchable=True)
def im(x) -> Any:
    """Imaginary part of complex numbers

    Args:
        x: A numeric vector

    Returns:
        The imaginary parts
    """
    raise _NotImplementedByCurrentBackendError("im", x)


@_register_func(pipeable=True, dispatchable=True)
def as_complex(x) -> Any:
    """Convert a vector to a complex vector

    Args:
        x: A numeric vector

    Returns:
        The complex vector
    """
    raise _NotImplementedByCurrentBackendError("as_complex", x)


@_register_func(pipeable=True, dispatchable=True)
def is_complex(x) -> Any:
    """Check if a vector is complex

    Args:
        x: A numeric vector

    Returns:
        Whether the vector is complex
    """
    raise _NotImplementedByCurrentBackendError("is_complex", x)


@_register_func(pipeable=True, dispatchable=True)
def cummax(x) -> Any:
    """Cumulative maxima

    Args:
        x: A numeric vector

    Returns:
        The cumulative maxima
    """
    raise _NotImplementedByCurrentBackendError("cummax", x)


@_register_func(pipeable=True, dispatchable=True)
def cummin(x) -> Any:
    """Cumulative minima

    Args:
        x: A numeric vector

    Returns:
        The cumulative minima
    """
    raise _NotImplementedByCurrentBackendError("cummin", x)


@_register_func(pipeable=True, dispatchable=True)
def cumprod(x) -> Any:
    """Cumulative products

    Args:
        x: A numeric vector

    Returns:
        The cumulative products
    """
    raise _NotImplementedByCurrentBackendError("cumprod", x)


@_register_func(pipeable=True, dispatchable=True)
def cumsum(x) -> Any:
    """Cumulative sums

    Args:
        x: A numeric vector

    Returns:
        The cumulative sums
    """
    raise _NotImplementedByCurrentBackendError("cumsum", x)


@_register_func(pipeable=True, dispatchable=True)
def droplevels(x) -> Any:
    """Drop unused levels of a factor

    Args:
        x: A numeric vector

    Returns:
        The factor vector
    """
    raise _NotImplementedByCurrentBackendError("droplevels", x)


@_register_func(pipeable=True, dispatchable=True)
def levels(x) -> Any:
    """Get the levels of a factor

    Args:
        x: A numeric vector

    Returns:
        The factor vector
    """
    raise _NotImplementedByCurrentBackendError("levels", x)


@_register_func(pipeable=True, dispatchable=True)
def set_levels(x, levels) -> Any:
    """Set the levels of a factor

    Args:
        x: A numeric vector
        levels: The new levels

    Returns:
        The factor vector
    """
    raise _NotImplementedByCurrentBackendError("set_levels", x)


@_register_func(pipeable=True, dispatchable=True)
def is_factor(x) -> Any:
    """Check if a vector is a factor

    Args:
        x: A numeric vector

    Returns:
        Whether the vector is a factor
    """
    raise _NotImplementedByCurrentBackendError("is_factor", x)


@_register_func(pipeable=True, dispatchable=True)
def is_ordered(x) -> Any:
    """Check if a vector is ordered

    Args:
        x: A numeric vector

    Returns:
        Whether the vector is ordered
    """
    raise _NotImplementedByCurrentBackendError("is_ordered", x)


@_register_func(pipeable=True, dispatchable=True)
def nlevels(x) -> Any:
    """Get the number of levels of a factor

    Args:
        x: A numeric vector

    Returns:
        The number of levels
    """
    raise _NotImplementedByCurrentBackendError("nlevels", x)


@_register_func(pipeable=True, dispatchable=True)
def factor(
    x=None,
    *,
    levels=None,
    labels=None,
    exclude=None,
    ordered=False,
    nmax=None,
) -> Any:
    """Create a factor vector

    Args:
        x: A numeric vector
        levels: The levels
        labels: The labels
        exclude: The excluded levels
        ordered: Whether the factor is ordered
        nmax: The maximum number of levels

    Returns:
        The factor vector
    """
    raise _NotImplementedByCurrentBackendError("factor", x)


@_register_func(pipeable=True, dispatchable=True)
def ordered(x, levels=None, labels=None, exclude=None, nmax=None) -> Any:
    """Create an ordered factor vector

    Args:
        x: A numeric vector
        levels: The levels
        labels: The labels
        exclude: The excluded levels
        nmax: The maximum number of levels

    Returns:
        The ordered factor vector
    """
    raise _NotImplementedByCurrentBackendError("ordered", x)


@_register_func(pipeable=True, dispatchable=True)
def cut(
    x,
    breaks,
    labels=None,
    include_lowest=False,
    right=True,
    dig_lab=3,
    ordered_result=False,
) -> Any:
    """Cut a numeric vector into bins

    Args:
        x: A numeric vector
        breaks: The breaks
        labels: The labels
        include_lowest: Whether to include the lowest value
        right: Whether to include the rightmost value
        dig_lab: The number of digits for labels
        ordered_result: Whether to return an ordered factor

    Returns:
        The factor vector
    """
    raise _NotImplementedByCurrentBackendError("cut", x)


@_register_func(pipeable=True, dispatchable=True)
def diff(x, lag: int = 1, differences: int = 1) -> Any:
    """Difference of a numeric vector

    Args:
        x: A numeric vector
        lag: The lag to use. Could be negative.
            It always calculates `x[lag:] - x[:-lag]` even when `lag` is
            negative
        differences: The order of the difference

    Returns:
        An array of `x[lag:] – x[:-lag]`.
        If `differences > 1`, the rule applies `differences` times on `x`
    """
    raise _NotImplementedByCurrentBackendError("diff", x)


@_register_func(pipeable=True, dispatchable=True)
def expand_grid(x, *args, **kwargs) -> Any:
    """Expand a grid

    Args:
        x: A numeric vector
        *args: Additional numeric vectors
        **kwargs: Additional keyword arguments

    Returns:
        The expanded grid
    """
    raise _NotImplementedByCurrentBackendError("expand_grid", x)


@_register_func(pipeable=True, dispatchable=True)
def outer(x, y, fun="*") -> Any:
    """Outer product of two vectors

    Args:
        x: A numeric vector
        y: A numeric vector
        fun: The function to handle how the result of the elements from
            the first and second vectors should be computed.
            The function has to be vectorized at the second argument, and
            return the same shape as y.

    Returns:
        The outer product
    """
    raise _NotImplementedByCurrentBackendError("outer", x)


@_register_func(cls=object, pipeable=True, dispatchable=True)
def make_names(names, unique: bool = True) -> Any:
    """Make names for a vector

    Args:
        names: character vector to be coerced to syntactically valid names.
            This is coerced to character if necessary.
        unique: Whether to make the names unique

    Returns:
        The names
    """
    try:
        from slugify import slugify
    except ImportError as imerr:  # pragma: no cover
        raise ValueError(
            "`make_names()` requires `python-slugify` package.\n"
            "Try: pip install -U slugify"
        ) from imerr

    if isinstance(names, str):
        names = [names]
    try:
        iter(names)
    except TypeError:
        names = [names]

    names = [
        slugify(str(name), separator="_", lowercase=False)
        for name in names
    ]
    names = [f"_{name}" if name[0].isdigit() else name for name in names]
    if unique:
        return _repair_names(names, "unique")
    return names


@_register_func(cls=object, pipeable=True, dispatchable=True)
def make_unique(names) -> Any:
    """Make a vector unique

    Args:
        names: a character vector

    Returns:
        The unique vector
    """
    return make_names(names, unique=True, __ast_fallback="normal")


@_register_func(pipeable=True, dispatchable=True)
def rank(x, na_last: bool = True, ties_method: str = "average") -> Any:
    """Rank a numeric vector

    Args:
        x: A numeric vector
        na_last: Whether to put NA at the end
        ties_method: The method to handle ties. One of "average", "first",
            "last", "random", "max", "min"

    Returns:
        The ranks
    """
    raise _NotImplementedByCurrentBackendError("rank", x)


@_register_func(cls=object, pipeable=True, dispatchable=True)
def identity(x) -> Any:
    """Identity function

    Args:
        x: A numeric vector

    Returns:
        The same vector
    """
    return x


@_register_func(pipeable=True, dispatchable=True)
def is_logical(x) -> Any:
    """Check if a vector is logical

    Args:
        x: A numeric vector

    Returns:
        Whether the vector is logical
    """
    raise _NotImplementedByCurrentBackendError("is_logical", x)


@_register_func(pipeable=True, dispatchable=True)
def is_true(x) -> bool:
    """Check if anything is true

    Args:
        x: object to be tested

    Returns:
        Whether `x` is true
    """
    raise _NotImplementedByCurrentBackendError("is_true", x)


@_register_func(pipeable=True, dispatchable=True)
def is_false(x) -> bool:
    """Check if anything is false

    Args:
        x: object to be tested

    Returns:
        Whether `x` is false
    """
    raise _NotImplementedByCurrentBackendError("is_false", x)


@_register_func(pipeable=True, dispatchable=True)
def is_na(x) -> Any:
    """Check if anything is NA

    Args:
        x: object to be tested

    Returns:
        Whether `x` is NA
    """
    raise _NotImplementedByCurrentBackendError("is_na", x)


@_register_func(pipeable=True, dispatchable=True)
def is_finite(x) -> Any:
    """Check if anything is finite

    Args:
        x: object to be tested

    Returns:
        Whether `x` is finite
    """
    raise _NotImplementedByCurrentBackendError("is_finite", x)


@_register_func(pipeable=True, dispatchable=True)
def is_infinite(x) -> Any:
    """Check if anything is infinite

    Args:
        x: object to be tested

    Returns:
        Whether `x` is infinite
    """
    raise _NotImplementedByCurrentBackendError("is_infinite", x)


@_register_func(pipeable=True, dispatchable=True)
def any_na(x) -> Any:
    """Check if anything in `x` is NA

    Args:
        x: object to be tested

    Returns:
        Whether anything in `x` is NA
    """
    raise _NotImplementedByCurrentBackendError("any_na", x)


@_register_func(pipeable=True, dispatchable=True)
def as_null(x) -> Any:
    """Convert anything to NULL

    Args:
        x: object to be converted

    Returns:
        NULL
    """
    raise _NotImplementedByCurrentBackendError("as_null", x)


@_register_func(pipeable=True, dispatchable=True)
def is_null(x) -> Any:
    """Check if anything is NULL

    Args:
        x: object to be tested

    Returns:
        Whether `x` is NULL
    """
    raise _NotImplementedByCurrentBackendError("is_null", x)


@_register_func(pipeable=True, dispatchable=True)
def set_seed(seed) -> Any:
    """Set the seed of the random number generator

    Args:
        seed: The seed
    """
    raise _NotImplementedByCurrentBackendError("set_seed", seed)


@_register_func(pipeable=True, dispatchable="all")
def rep(x, times=1, length=None, each=1) -> Any:
    """Replicate elements of a vector

    Args:
        x: a vector or scaler
        times: number of times to repeat each element if of length len(x),
            or to repeat the whole vector if of length 1
        length: non-negative integer. The desired length of the output vector
        each: non-negative integer. Each element of x is repeated each times.

    Returns:
        The replicated vector
    """
    raise _NotImplementedByCurrentBackendError("rep", x)


@_register_func(pipeable=True, dispatchable=True)
def c_(*args) -> Any:
    """Concatenate vectors

    Args:
        args: vectors to be concatenated

    Returns:
        The concatenated vector
    """
    raise _NotImplementedByCurrentBackendError("c", *args)


c = _CollectionFunction(c_)


@_register_func(pipeable=True, dispatchable=True)
def length(x) -> Any:
    """Get the length of a vector

    Args:
        x: a vector or scaler

    Returns:
        The length of the vector
    """
    raise _NotImplementedByCurrentBackendError("length", x)


@_register_func(pipeable=True, dispatchable=True)
def lengths(x) -> Any:
    """Get the lengths of a list

    Args:
        x: a list

    Returns:
        The lengths of the list
    """
    raise _NotImplementedByCurrentBackendError("lengths", x)


@_register_func(pipeable=True, dispatchable=True)
def order(x, decreasing: bool = False, na_last: bool = True) -> Any:
    """Order a vector

    Args:
        x: a vector or scaler
        decreasing: Whether to order in decreasing order
        na_last: Whether to put NA at the end

    Returns:
        The order
    """
    raise _NotImplementedByCurrentBackendError("order", x)


@_register_func(pipeable=True, dispatchable=True)
def sort(x, decreasing: bool = False, na_last: bool = True) -> Any:
    """Sort a vector

    Args:
        x: a vector or scaler
        decreasing: Whether to sort in decreasing order
        na_last: Whether to put NA at the end

    Returns:
        The sorted vector
    """
    raise _NotImplementedByCurrentBackendError("sort", x)


@_register_func(pipeable=True, dispatchable=True)
def rev(x) -> Any:
    """Reverse a vector

    Args:
        x: a vector or scaler

    Returns:
        The reversed vector
    """
    raise _NotImplementedByCurrentBackendError("rev", x)


@_register_func(pipeable=True, dispatchable=True)
def sample(x, size=None, replace: bool = False, prob=None) -> Any:
    """Sample a vector

    Args:
        x: a vector or scaler
        size: the size of the sample
        replace: whether to sample with replacement
        prob: the probabilities of sampling each element

    Returns:
        The sampled vector
    """
    raise _NotImplementedByCurrentBackendError("sample", x)


@_register_func(pipeable=True, dispatchable=True)
def seq(from_=None, to=None, by=None, length_out=None, along_with=None) -> Any:
    """Generate a sequence

    Args:
        from_: the start of the sequence
        to: the end of the sequence
        by: the step of the sequence
        length_out: the length of the sequence
        along_with: the sequence to be aligned with

    Returns:
        The sequence
    """
    raise _NotImplementedByCurrentBackendError("seq", from_)


@_register_func(pipeable=True, dispatchable=True)
def seq_along(x) -> Any:
    """Generate a sequence along a vector

    Args:
        x: a vector or scaler

    Returns:
        The sequence
    """
    raise _NotImplementedByCurrentBackendError("seq_along", x)


@_register_func(pipeable=True, dispatchable=True)
def seq_len(x) -> Any:
    """Generate a sequence of length x

    Args:
        x: a vector or scaler

    Returns:
        The sequence
    """
    raise _NotImplementedByCurrentBackendError("seq_len", x)


@_register_func(pipeable=True, dispatchable=True)
def match(x, table, nomatch=-1) -> Any:
    """Match elements of a vector

    Args:
        x: a vector or scaler
        table: the table to match
        nomatch: the value to use for no match

    Returns:
        The matched vector
    """
    raise _NotImplementedByCurrentBackendError("match", x)


@_register_func(pipeable=True, dispatchable=True)
def beta(x, y) -> Any:
    """Compute the beta function

    Args:
        x: a vector or scaler
        y: a vector or scaler

    Returns:
        The beta function
    """
    raise _NotImplementedByCurrentBackendError("beta", x)


@_register_func(pipeable=True, dispatchable=True)
def lgamma(x) -> Any:
    """Compute the log gamma function

    Args:
        x: a vector or scaler

    Returns:
        The log gamma function
    """
    raise _NotImplementedByCurrentBackendError("lgamma", x)


@_register_func(pipeable=True, dispatchable=True)
def digamma(x) -> Any:
    """Compute the digamma function

    Args:
        x: a vector or scaler

    Returns:
        The digamma function
    """
    raise _NotImplementedByCurrentBackendError("digamma", x)


@_register_func(pipeable=True, dispatchable=True)
def trigamma(x) -> Any:
    """Compute the trigamma function

    Args:
        x: a vector or scaler

    Returns:
        The trigamma function
    """
    raise _NotImplementedByCurrentBackendError("trigamma", x)


@_register_func(pipeable=True, dispatchable=True)
def choose(n, k) -> Any:
    """Compute the binomial coefficient

    Args:
        n: a vector or scaler
        k: a vector or scaler

    Returns:
        The binomial coefficient
    """
    raise _NotImplementedByCurrentBackendError("choose", n)


@_register_func(pipeable=True, dispatchable=True)
def factorial(x) -> Any:
    """Compute the factorial

    Args:
        x: a vector or scaler

    Returns:
        The factorial
    """
    raise _NotImplementedByCurrentBackendError("factorial", x)


@_register_func(pipeable=True, dispatchable=True)
def gamma(x) -> Any:
    """Compute the gamma function

    Args:
        x: a vector or scaler

    Returns:
        The gamma function
    """
    raise _NotImplementedByCurrentBackendError("gamma", x)


@_register_func(pipeable=True, dispatchable=True)
def lfactorial(x) -> Any:
    """Compute the log factorial

    Args:
        x: a vector or scaler

    Returns:
        The log factorial
    """
    raise _NotImplementedByCurrentBackendError("lfactorial", x)


@_register_func(pipeable=True, dispatchable=True)
def lchoose(n, k) -> Any:
    """Compute the log binomial coefficient

    Args:
        n: a vector or scaler
        k: a vector or scaler

    Returns:
        The log binomial coefficient
    """
    raise _NotImplementedByCurrentBackendError("lchoose", n)


@_register_func(pipeable=True, dispatchable=True)
def lbeta(x, y) -> Any:
    """Compute the log beta function

    Args:
        x: a vector or scaler
        y: a vector or scaler

    Returns:
        The log beta function
    """
    raise _NotImplementedByCurrentBackendError("lbeta", x)


@_register_func(pipeable=True, dispatchable=True)
def psigamma(x, deriv) -> Any:
    """Compute the psi function

    Args:
        x: a vector or scaler
        deriv: the derivative

    Returns:
        The psi function
    """
    raise _NotImplementedByCurrentBackendError("psigamma", x)


@_register_func(pipeable=True, dispatchable=True)
def rnorm(n, mean=0, sd=1) -> Any:
    """Generate random normal variables

    Args:
        n: the number of random variables
        mean: the mean of the random variables
        sd: the standard deviation of the random variables

    Returns:
        The random normal variables
    """
    raise _NotImplementedByCurrentBackendError("rnorm", n)


@_register_func(pipeable=True, dispatchable=True)
def runif(n, min=0, max=1) -> Any:
    """Generate random uniform variables

    Args:
        n: the number of random variables
        min: the minimum of the random variables
        max: the maximum of the random variables

    Returns:
        The random uniform variables
    """
    raise _NotImplementedByCurrentBackendError("runif", n)


@_register_func(pipeable=True, dispatchable=True)
def rpois(n, lambda_) -> Any:
    """Generate random Poisson variables

    Args:
        n: the number of random variables
        lambda_: the lambda of the random variables

    Returns:
        The random Poisson variables
    """
    raise _NotImplementedByCurrentBackendError("rpois", n)


@_register_func(pipeable=True, dispatchable=True)
def rbinom(n, size, prob) -> Any:
    """Generate random binomial variables

    Args:
        n: the number of random variables
        size: the size of the random variables
        prob: the probability of the random variables

    Returns:
        The random binomial variables
    """
    raise _NotImplementedByCurrentBackendError("rbinom", n)


@_register_func(pipeable=True, dispatchable=True)
def rcauchy(n, location=0, scale=1) -> Any:
    """Generate random Cauchy variables

    Args:
        n: the number of random variables
        location: the location of the random variables
        scale: the scale of the random variables

    Returns:
        The random Cauchy variables
    """
    raise _NotImplementedByCurrentBackendError("rcauchy", n)


@_register_func(pipeable=True, dispatchable=True)
def rchisq(n, df) -> Any:
    """Generate random chi-squared variables

    Args:
        n: the number of random variables
        df: the degrees of freedom of the random variables

    Returns:
        The random chi-squared variables
    """
    raise _NotImplementedByCurrentBackendError("rchisq", n)


@_register_func(pipeable=True, dispatchable=True)
def rexp(n, rate) -> Any:
    """Generate random exponential variables

    Args:
        n: the number of random variables
        rate: the rate of the random variables

    Returns:
        The random exponential variables
    """
    raise _NotImplementedByCurrentBackendError("rexp", n)


@_register_func(pipeable=True, dispatchable=True)
def is_character(x) -> Any:
    """Is x a character vector

    Args:
        x: a vector or scaler

    Returns:
        True if x is a character vector
    """
    raise _NotImplementedByCurrentBackendError("is_character", x)


@_register_func(pipeable=True, dispatchable=True)
def grep(
    pattern,
    x,
    ignore_case=False,
    value=False,
    fixed=False,
    invert=False,
) -> Any:
    """Grep for a pattern

    Args:
        pattern: the pattern to search for
        x: the vector to search
        ignore_case: ignore case
        value: return the value
        fixed: use fixed string matching
        invert: invert the match

    Returns:
        The indices of the matches
    """
    raise _NotImplementedByCurrentBackendError("grep", pattern)


@_register_func(pipeable=True, dispatchable=True)
def grepl(pattern, x, ignore_case=False, fixed=False) -> Any:
    """Grep for a pattern

    Args:
        pattern: the pattern to search for
        x: the vector to search
        ignore_case: ignore case
        fixed: use fixed string matching

    Returns:
        The indices of the matches
    """
    raise _NotImplementedByCurrentBackendError("grepl", pattern)


@_register_func(pipeable=True, dispatchable=True)
def sub(pattern, replacement, x, ignore_case=False, fixed=False) -> Any:
    """Substitute a pattern

    Args:
        pattern: the pattern to search for
        replacement: the replacement
        x: the vector to search
        ignore_case: ignore case
        fixed: use fixed string matching

    Returns:
        The vector with the substitutions
    """
    raise _NotImplementedByCurrentBackendError("sub", pattern)


@_register_func(pipeable=True, dispatchable=True)
def gsub(pattern, replacement, x, ignore_case=False, fixed=False) -> Any:
    """Substitute a pattern

    Args:
        pattern: the pattern to search for
        replacement: the replacement
        x: the vector to search
        ignore_case: ignore case
        fixed: use fixed string matching

    Returns:
        The vector with the substitutions
    """
    raise _NotImplementedByCurrentBackendError("gsub", pattern)


@_register_func(pipeable=True, dispatchable=True)
def strsplit(x, split, fixed=False, perl=False, use_bytes=False) -> Any:
    """Split a string

    Args:
        x: the vector to split
        split: the pattern to split on
        fixed: use fixed string matching
        perl: use perl regular expressions
        use_bytes: use bytes

    Returns:
        The vector with the splits
    """
    raise _NotImplementedByCurrentBackendError("strsplit", x)


@_register_func(pipeable=True, dispatchable=True)
def paste(*args, sep=" ", collapse=None) -> Any:
    """Join a vector into a string

    Args:
        *args: the vector to join
        sep: the separator
        collapse: collapse the vector

    Returns:
        The vector joined into a string
    """
    raise _NotImplementedByCurrentBackendError("paste")


@_register_func(pipeable=True, dispatchable=True)
def paste0(*args, collapse=None) -> Any:
    """Join a vector into a string

    Args:
        *args: the vector to join
        collapse: collapse the vector

    Returns:
        The vector joined into a string
    """
    raise _NotImplementedByCurrentBackendError("paste0")


@_register_func(pipeable=True, dispatchable=True)
def sprintf(fmt, *args) -> Any:
    """Format a string

    Args:
        fmt: the format string
        args: the arguments to the format string

    Returns:
        The formatted string
    """
    raise _NotImplementedByCurrentBackendError("sprintf", fmt)


@_register_func(pipeable=True, dispatchable=True)
def substr(x, start, stop) -> Any:
    """Get a substring

    Args:
        x: the string to get the substring from
        start: the start of the substring
        stop: the stop of the substring

    Returns:
        The substring
    """
    raise _NotImplementedByCurrentBackendError("substr", x)


@_register_func(pipeable=True, dispatchable=True)
def substring(x, first, last=None) -> Any:
    """Get a substring

    Args:
        x: the string to get the substring from
        first: the start of the substring
        last: the stop of the substring

    Returns:
        The substring
    """
    raise _NotImplementedByCurrentBackendError("substring", x)


@_register_func(pipeable=True, dispatchable=True)
def startswith(x, prefix) -> Any:
    """Does x start with prefix

    Args:
        x: the string to check
        prefix: the prefix to check

    Returns:
        True if x starts with prefix
    """
    raise _NotImplementedByCurrentBackendError("startswith", x)


@_register_func(pipeable=True, dispatchable=True)
def endswith(x, suffix) -> Any:
    """Does x end with suffix

    Args:
        x: the string to check
        suffix: the suffix to check

    Returns:
        True if x ends with suffix
    """
    raise _NotImplementedByCurrentBackendError("endswith", x)


@_register_func(pipeable=True, dispatchable=True)
def strtoi(x, base=0) -> Any:
    """Convert a string to an integer

    Args:
        x: the string to convert
        base: the base of the integer

    Returns:
        The integer
    """
    raise _NotImplementedByCurrentBackendError("strtoi", x)


@_register_func(pipeable=True, dispatchable=True)
def trimws(x, which="both", whitespace=r" \t") -> Any:
    """Trim whitespace from a string

    Args:
        x: the string to trim
        which: which whitespace to trim
        whitespace: the whitespace to trim

    Returns:
        The trimmed string
    """
    raise _NotImplementedByCurrentBackendError("trimws", x)


@_register_func(pipeable=True, dispatchable=True)
def toupper(x) -> Any:
    """Convert a string to upper case

    Args:
        x: the string to convert

    Returns:
        The upper case string
    """
    raise _NotImplementedByCurrentBackendError("toupper", x)


@_register_func(pipeable=True, dispatchable=True)
def tolower(x) -> Any:
    """Convert a string to lower case

    Args:
        x: the string to convert

    Returns:
        The lower case string
    """
    raise _NotImplementedByCurrentBackendError("tolower", x)


@_register_func(pipeable=True, dispatchable=True)
def chartr(old, new, x) -> Any:
    """Translate characters

    Args:
        old: the characters to translate
        new: the new characters
        x: the string to translate

    Returns:
        The translated string
    """
    raise _NotImplementedByCurrentBackendError("chartr", x)


@_register_func(pipeable=True, dispatchable=True)
def nchar(
    x,
    type_="width",
    allow_na: bool = True,
    keep_na: bool = False,
    _na_len: int = 2,
) -> Any:
    """Get the number of characters in a string

    Args:
        x: the string to count
        type: the type of count
        allow_na: allow NA
        keep_na: keep NA

    Returns:
        The number of characters
    """
    raise _NotImplementedByCurrentBackendError("nchar", x)


@_register_func(pipeable=True, dispatchable=True)
def nzchar(x, keep_na: bool = False) -> Any:
    """Is the string non-zero length

    Args:
        x: the string to check
        keep_na: keep NA

    Returns:
        True if the string is non-zero length
    """
    raise _NotImplementedByCurrentBackendError("nzchar", x)


@_register_func(pipeable=True, dispatchable=True)
def table(
    x,
    *more,
    exclude=None,
    use_na="no",
    dnn=None,
    deparse_level=1,
) -> Any:
    """Get the table of a vector

    Args:
        x: the vector to get the table of
        more: more vectors
        exclude: exclude these values
        use_na: use NA
        dnn: the names of the vectors
        deparse_level: the deparse level

    Returns:
        The table
    """
    raise _NotImplementedByCurrentBackendError("table", x)


@_register_func(pipeable=True, dispatchable=True)
def tabulate(bin, nbins=None) -> Any:
    """Get the table of a vector

    Args:
        bin: the vector to get the table of
        nbins: the number of bins

    Returns:
        An integer valued 'integer' vector (without names).
        There is a bin for each of the values '1, ..., nbins'
    """
    raise _NotImplementedByCurrentBackendError("tabulate", bin)


@_register_func(pipeable=True, dispatchable=True)
def is_atomic(x) -> Any:
    """Is the object atomic

    Args:
        x: the object to check

    Returns:
        True if the object is atomic
    """
    raise _NotImplementedByCurrentBackendError("is_atomic", x)


@_register_func(pipeable=True, dispatchable=True)
def is_double(x) -> Any:
    """Is the object a double

    Args:
        x: the object to check

    Returns:
        True if the object is a double
    """
    raise _NotImplementedByCurrentBackendError("is_double", x)


@_register_func(pipeable=True, dispatchable=True)
def is_element(x, y) -> Any:
    """Is the object an element of the table

    Args:
        x: the object to check
        y: the pool to check

    Returns:
        True if the object is an element of the pool
    """
    raise _NotImplementedByCurrentBackendError("is_element", x)


is_in = is_element


@_register_func(pipeable=True, dispatchable=True)
def is_integer(x) -> Any:
    """Is the object an integer

    Args:
        x: the object to check

    Returns:
        True if the object is an integer
    """
    raise _NotImplementedByCurrentBackendError("is_integer", x)


@_register_func(pipeable=True, dispatchable=True)
def is_numeric(x) -> Any:
    """Is the object numeric

    Args:
        x: the object to check

    Returns:
        True if the object is numeric
    """
    raise _NotImplementedByCurrentBackendError("is_numeric", x)


@_register_func(pipeable=True, dispatchable=True)
def any_(x, na_rm: bool = False) -> Any:
    """Is any element true

    Args:
        x: the vector to check
        na_rm: remove NA

    Returns:
        True if any element is true
    """
    raise _NotImplementedByCurrentBackendError("any", x)


@_register_func(pipeable=True, dispatchable=True)
def all_(x, na_rm: bool = False) -> Any:
    """Are all elements true

    Args:
        x: the vector to check
        na_rm: remove NA

    Returns:
        True if all elements are true
    """
    raise _NotImplementedByCurrentBackendError("all", x)


@_register_func(pipeable=True, dispatchable=True)
def acos(x) -> Any:
    """Get the inverse cosine

    Args:
        x: the value to get the inverse cosine of

    Returns:
        The inverse cosine
    """
    raise _NotImplementedByCurrentBackendError("acos", x)


@_register_func(pipeable=True, dispatchable=True)
def acosh(x) -> Any:
    """Get the inverse hyperbolic cosine

    Args:
        x: the value to get the inverse hyperbolic cosine of

    Returns:
        The inverse hyperbolic cosine
    """
    raise _NotImplementedByCurrentBackendError("acosh", x)


@_register_func(pipeable=True, dispatchable=True)
def asin(x) -> Any:
    """Get the inverse sine

    Args:
        x: the value to get the inverse sine of

    Returns:
        The inverse sine
    """
    raise _NotImplementedByCurrentBackendError("asin", x)


@_register_func(pipeable=True, dispatchable=True)
def asinh(x) -> Any:
    """Get the inverse hyperbolic sine

    Args:
        x: the value to get the inverse hyperbolic sine of

    Returns:
        The inverse hyperbolic sine
    """
    raise _NotImplementedByCurrentBackendError("asinh", x)


@_register_func(pipeable=True, dispatchable=True)
def atan(x) -> Any:
    """Get the inverse tangent

    Args:
        x: the value to get the inverse tangent of

    Returns:
        The inverse tangent
    """
    raise _NotImplementedByCurrentBackendError("atan", x)


@_register_func(pipeable=True, dispatchable=True)
def atanh(x) -> Any:
    """Get the inverse hyperbolic tangent

    Args:
        x: the value to get the inverse hyperbolic tangent of

    Returns:
        The inverse hyperbolic tangent
    """
    raise _NotImplementedByCurrentBackendError("atanh", x)


@_register_func(pipeable=True, dispatchable=True)
def cos(x) -> Any:
    """Get the cosine

    Args:
        x: the value to get the cosine of

    Returns:
        The cosine
    """
    raise _NotImplementedByCurrentBackendError("cos", x)


@_register_func(pipeable=True, dispatchable=True)
def cosh(x) -> Any:
    """Get the hyperbolic cosine

    Args:
        x: the value to get the hyperbolic cosine of

    Returns:
        The hyperbolic cosine
    """
    raise _NotImplementedByCurrentBackendError("cosh", x)


@_register_func(pipeable=True, dispatchable=True)
def cospi(x) -> Any:
    """Get the cosine of pi times x

    Args:
        x: the value to get the cosine of pi times x of

    Returns:
        The cosine of pi times x
    """
    raise _NotImplementedByCurrentBackendError("cospi", x)


@_register_func(pipeable=True, dispatchable=True)
def sin(x) -> Any:
    """Get the sine

    Args:
        x: the value to get the sine of

    Returns:
        The sine
    """
    raise _NotImplementedByCurrentBackendError("sin", x)


@_register_func(pipeable=True, dispatchable=True)
def sinh(x) -> Any:
    """Get the hyperbolic sine

    Args:
        x: the value to get the hyperbolic sine of

    Returns:
        The hyperbolic sine
    """
    raise _NotImplementedByCurrentBackendError("sinh", x)


@_register_func(pipeable=True, dispatchable=True)
def sinpi(x) -> Any:
    """Get the sine of pi times x

    Args:
        x: the value to get the sine of pi times x of

    Returns:
        The sine of pi times x
    """
    raise _NotImplementedByCurrentBackendError("sinpi", x)


@_register_func(pipeable=True, dispatchable=True)
def tan(x) -> Any:
    """Get the tangent

    Args:
        x: the value to get the tangent of

    Returns:
        The tangent
    """
    raise _NotImplementedByCurrentBackendError("tan", x)


@_register_func(pipeable=True, dispatchable=True)
def tanh(x) -> Any:
    """Get the hyperbolic tangent

    Args:
        x: the value to get the hyperbolic tangent of

    Returns:
        The hyperbolic tangent
    """
    raise _NotImplementedByCurrentBackendError("tanh", x)


@_register_func(pipeable=True, dispatchable=True)
def tanpi(x) -> Any:
    """Get the tangent of pi times x

    Args:
        x: the value to get the tangent of pi times x of

    Returns:
        The tangent of pi times x
    """
    raise _NotImplementedByCurrentBackendError("tanpi", x)


@_register_func(pipeable=True, dispatchable=True)
def atan2(y, x) -> Any:
    """Get the inverse tangent of y/x

    Args:
        y: the numerator
        x: the denominator

    Returns:
        The inverse tangent of y/x
    """
    raise _NotImplementedByCurrentBackendError("atan2", x)


@_register_func(pipeable=True, dispatchable=True)
def append(x, values, after: int = -1) -> Any:
    """Append values to the vector

    Args:
        x: the vector to append to
        values: the values to append
        after: the index to append after

    Returns:
        The vector with the values appended
    """
    raise _NotImplementedByCurrentBackendError("append", x)


@_register_func(pipeable=True, dispatchable=True)
def colnames(x, nested: bool = True) -> Any:
    """Get the column names

    Args:
        x: the data frame to get the column names of
        nested: whether x is a nested data frame

    Returns:
        The column names
    """
    raise _NotImplementedByCurrentBackendError("colnames", x)


@_register_func(pipeable=True, dispatchable=True)
def set_colnames(x, names, nested: bool = True) -> Any:
    """Set the column names

    Args:
        x: the data frame to set the column names of
        names: the column names to set
        nested: whether the frame are nested

    Returns:
        The data frame with the column names set
    """
    raise _NotImplementedByCurrentBackendError("set_colnames", x)


@_register_func(pipeable=True, dispatchable=True)
def rownames(x) -> Any:
    """Get the row names

    Args:
        x: the data frame to get the row names of

    Returns:
        The row names
    """
    raise _NotImplementedByCurrentBackendError("rownames", x)


@_register_func(pipeable=True, dispatchable=True)
def set_rownames(x, names) -> Any:
    """Set the row names

    Args:
        x: the data frame to set the row names of
        names: the row names to set

    Returns:
        The data frame with the row names set
    """
    raise _NotImplementedByCurrentBackendError("set_rownames", x)


@_register_func(pipeable=True, dispatchable=True)
def dim(x, nested: bool = True) -> Any:
    """Get the dimensions

    Args:
        x: the data frame to get the dimensions of
        nested: whether x is a nested data frame

    Returns:
        The dimensions
    """
    raise _NotImplementedByCurrentBackendError("dim", x)


@_register_func(pipeable=True, dispatchable=True)
def diag(x, nrow=None, ncol=None) -> Any:
    """Get the diagonal of a matrix

    Args:
        x: the matrix to get the diagonal of
        nrow: the number of rows
        ncol: the number of columns

    Returns:
        The diagonal of the matrix
    """
    raise _NotImplementedByCurrentBackendError("diag", x)


@_register_func(pipeable=True, dispatchable=True)
def duplicated(x, incomparables=None, from_last: bool = False) -> Any:
    """Get the duplicated values

    Args:
        x: the vector to get the duplicated values of
        incomparables: the incomparables
        from_last: whether to search from the last

    Returns:
        The duplicated values
    """
    raise _NotImplementedByCurrentBackendError("duplicated", x)


@_register_func(pipeable=True, dispatchable=True)
def intersect(x, y) -> Any:
    """Get the intersection of two vectors

    Args:
        x: the first vector
        y: the second vector

    Returns:
        The intersection of the two vectors
    """
    raise _NotImplementedByCurrentBackendError("intersect", x)


@_register_func(pipeable=True, dispatchable=True)
def ncol(x, nested: bool = True) -> Any:
    """Get the number of columns

    Args:
        x: the data frame to get the number of columns of
        nested: whether x is a nested data frame

    Returns:
        The number of columns
    """
    raise _NotImplementedByCurrentBackendError("ncol", x)


@_register_func(pipeable=True, dispatchable=True)
def nrow(x) -> Any:
    """Get the number of rows

    Args:
        x: the data frame to get the number of rows of

    Returns:
        The number of rows
    """
    raise _NotImplementedByCurrentBackendError("nrow", x)


@_register_func(pipeable=True, dispatchable=True)
def proportions(x, margin: int = 1) -> Any:
    """Get the proportion table

    Args:
        x: the data frame to get the proportion table of
        margin: the margin

    Returns:
        The proportion table
    """
    raise _NotImplementedByCurrentBackendError("proportions", x)


@_register_func(pipeable=True, dispatchable=True)
def setdiff(x, y) -> Any:
    """Get the difference of two vectors

    Args:
        x: the first vector
        y: the second vector

    Returns:
        The difference of the two vectors
    """
    raise _NotImplementedByCurrentBackendError("setdiff", x)


@_register_func(pipeable=True, dispatchable=True)
def setequal(x, y) -> Any:
    """Check if two vectors are equal

    Args:
        x: the first vector
        y: the second vector

    Returns:
        Whether the two vectors are equal
    """
    raise _NotImplementedByCurrentBackendError("setequal", x)


@_register_func(pipeable=True, dispatchable=True)
def unique(x) -> Any:
    """Get the unique values

    Args:
        x: the vector to get the unique values of

    Returns:
        The unique values
    """
    raise _NotImplementedByCurrentBackendError("unique", x)


@_register_func(pipeable=True, dispatchable=True)
def t(x) -> Any:
    """Get the transpose

    Args:
        x: the matrix to get the transpose of

    Returns:
        The transpose
    """
    raise _NotImplementedByCurrentBackendError("t", x)


@_register_func(pipeable=True, dispatchable=True)
def union(x, y) -> Any:
    """Get the union of two vectors

    Args:
        x: the first vector
        y: the second vector

    Returns:
        The union of the two vectors
    """
    raise _NotImplementedByCurrentBackendError("union", x)


@_register_func(pipeable=True, dispatchable=True)
def max_col(x, ties_method: str = "random", nested: bool = True) -> Any:
    """Get the maximum column

    Args:
        x: the data frame to get the maximum column of
        ties_method: the ties method
        nested: whether x is a nested data frame

    Returns:
        The maximum column
    """
    raise _NotImplementedByCurrentBackendError("max_col", x)


@_register_func(pipeable=True, dispatchable=True)
def complete_cases(x) -> Any:
    """Get the complete cases

    Args:
        x: the data frame to get the complete cases of

    Returns:
        The complete cases
    """
    raise _NotImplementedByCurrentBackendError("complete_cases", x)


@_register_func(pipeable=True, dispatchable=True)
def head(x, n: int = 6) -> Any:
    """Get the first n rows

    Args:
        x: the data frame to get the first n rows of
        n: the number of rows to get

    Returns:
        The first n rows
    """
    raise _NotImplementedByCurrentBackendError("head", x)


@_register_func(pipeable=True, dispatchable=True)
def tail(x, n: int = 6) -> Any:
    """Get the last n rows

    Args:
        x: the data frame to get the last n rows of
        n: the number of rows to get

    Returns:
        The last n rows
    """
    raise _NotImplementedByCurrentBackendError("tail", x)


@_register_func(pipeable=True, dispatchable=True)
def which(x) -> Any:
    """Get the indices of the non-zero values

    Args:
        x: the vector to get the indices of the non-zero values of

    Returns:
        The indices of the non-zero values
    """
    raise _NotImplementedByCurrentBackendError("which", x)


@_register_func(pipeable=True, dispatchable=True)
def which_max(x) -> Any:
    """Get the index of the maximum value

    Args:
        x: the vector to get the index of the maximum value of

    Returns:
        The index of the maximum value
    """
    raise _NotImplementedByCurrentBackendError("which_max", x)


@_register_func(pipeable=True, dispatchable=True)
def which_min(x) -> Any:
    """Get the index of the minimum value

    Args:
        x: the vector to get the index of the minimum value of

    Returns:
        The index of the minimum value
    """
    raise _NotImplementedByCurrentBackendError("which_min", x)
