from functools import singledispatch
from typing import Any, Callable, Mapping, Union

from pipda import register_func, ReferenceAttr, ReferenceItem
from pandas._typing import Dtype
from pandas.core.indexes.range import RangeIndex
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy

from ..core.tibble import Tibble, TibbleGroupby, TibbleRowwise
from ..core.contexts import Context


# Register as a function, so that
# tibble(a=[1,2,3], b=f.a / sum(f.a)) can work
# Otherwise sum(f.a) will do fast eval
# Note that when use it as a argument of a verb
# The expression is evaluated using the verb's data
@register_func(None, context=Context.EVAL)
def tibble(
    *args,
    _name_repair: Union[str, Callable] = "check_unique",
    _rows: int = None,
    _dtypes: Union[Dtype, Mapping[str, Dtype]] = None,
    **kwargs,
) -> Tibble:
    """Constructs a data frame

    Args:
        *args: and
        **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        _rows: Number of rows of a 0-col dataframe when args and kwargs are
            not provided. When args or kwargs are provided, this is ignored.
        _dtypes: The dtypes for each columns to convert to.

    Returns:
        A constructed tibble
    """
    return Tibble.from_args(
        *args,
        **kwargs,
        _name_repair=_name_repair,
        _rows=_rows,
        _dtypes=_dtypes,
    )


def tribble(
    *dummies: Any,
    _name_repair: Union[str, Callable] = "minimal",
    _dtypes: Union[Dtype, Mapping[str, Dtype]] = None,
) -> Tibble:
    """Create dataframe using an easier to read row-by-row layout
    Unlike original API that uses formula (`f.col`) to indicate the column
    names, we use `f.col` to indicate them.

    Args:
        *dummies: Arguments specifying the structure of a dataframe
            Variable names should be specified with `f.name`
        _dtypes: The dtypes for each columns to convert to.

    Examples:
        >>> tribble(
        >>>     f.colA, f.colB,
        >>>     "a",    1,
        >>>     "b",    2,
        >>>     "c",    3,
        >>> )

    Returns:
        A dataframe
    """
    columns = []
    data = []
    for i, dummy in enumerate(dummies):
        # columns
        if (
            isinstance(dummy, (ReferenceAttr, ReferenceItem))
            and dummy._pipda_level == 1
        ):
            columns.append(dummy._pipda_ref)

        elif not columns:
            raise ValueError(
                "Must specify at least one column using the `f.<name>` syntax."
            )

        else:
            ncols = len(columns)
            if not data:
                data = [[] for _ in range(ncols)]

            data[i % ncols].append(dummy)

    # only columns provided
    if not data:
        data = [[] for _ in range(len(columns))]

    if len(data[-1]) != len(data[0]):
        raise ValueError(
            "Data must be rectangular. "
            f"{sum(len(dat) for dat in data)} cells is not an integer "
            f"multiple of {len(columns)} columns."
        )

    return Tibble.from_pairs(
        columns,
        data,
        _name_repair=_name_repair,
        _dtypes=_dtypes,
    )


@register_func(None, context=Context.EVAL)
def tibble_row(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    _dtypes: Union[Dtype, Mapping[str, Dtype]] = None,
    **kwargs: Any,
) -> Tibble:
    """Constructs a data frame that is guaranteed to occupy one row.
    Scalar values will be wrapped with `[]`
    Args:
        *args: and
        **kwargs: A set of name-value pairs.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
    Returns:
        A constructed dataframe
    """
    if not args and not kwargs:
        df = Tibble(index=[0])  # still one row
    else:
        df = tibble(*args, **kwargs, _name_repair=_name_repair, _dtypes=_dtypes)

    if df.shape[0] > 1:
        raise ValueError("All arguments must be size one, use `[]` to wrap.")

    return df


@singledispatch
def as_tibble(df: Any) -> Tibble:
    """Convert a pandas DataFrame object to Tibble object"""
    return Tibble(df)


@as_tibble.register
def _(df: DataFrameGroupBy) -> TibbleGroupby:
    """Convert a pandas DataFrame object to TibbleGroupBy object"""
    klass = TibbleRowwise if is_rowwise(df) else TibbleGroupby
    return klass(
        df.obj,
        meta={
            "grouped": df,
            "group_vars": [
                name for name in df.grouper.names if name is not None
            ]
        }
    )


@as_tibble.register
def _(df: Tibble) -> Tibble:
    """Convert a pandas DataFrame object to TibbleGroupBy object"""
    return df


def is_rowwise(
    x: Union[SeriesGroupBy, DataFrameGroupBy, TibbleGroupby]
) -> bool:
    if isinstance(x, TibbleGroupby):
        return is_rowwise(x._datar_meta["grouped"])

    return (
        x.grouper.result_index.size == len(x)
        and isinstance(x.grouper.result_index, RangeIndex)
    )
