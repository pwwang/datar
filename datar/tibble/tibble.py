from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable, Mapping

from pipda import register_func, register_verb, ReferenceAttr, ReferenceItem

from ..core.backends.pandas.api.types import is_scalar
from ..core.backends.pandas.core.groupby import DataFrameGroupBy

from ..core.tibble import Tibble, TibbleGrouped
from ..core.contexts import Context

if TYPE_CHECKING:
    from pandas._typing import Dtype


def tibble(
    *args,
    _name_repair: str | Callable = "check_unique",
    _rows: int = None,
    _dtypes: Dtype | Mapping[str, Dtype] = None,
    _drop_index: bool = False,
    _index=None,
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
        _drop_index: Whether drop the index for the final data frame
        _index: The new index of the output frame

    Returns:
        A constructed tibble
    """
    out = Tibble.from_args(
        *args,
        **kwargs,
        _name_repair=_name_repair,
        _rows=_rows,
        _dtypes=_dtypes,
    )
    if _drop_index:
        return out.reset_index(drop=True)

    if _index is not None:
        out.index = [_index] if is_scalar(_index) else _index

    return out


# Like tibble, but works as a registered function, so that it can take
# expressions that will be evaluated by verb data
fibble = register_func(tibble, context=Context.EVAL)


def tribble(
    *dummies: Any,
    _name_repair: str | Callable = "minimal",
    _dtypes: Dtype | Mapping[str, Dtype] = None,
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


@register_func(context=Context.EVAL)
def tibble_row(
    *args: Any,
    _name_repair: str | Callable = "check_unique",
    _dtypes: Dtype | Mapping[str, Dtype] = None,
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
        df = Tibble(index=range(1))  # still one row
    else:
        df = tibble(*args, **kwargs, _name_repair=_name_repair, _dtypes=_dtypes)

    if df.shape[0] > 1:
        raise ValueError("All arguments must be size one, use `[]` to wrap.")

    return df


@register_verb(object, context=Context.EVAL)
def as_tibble(df: Any) -> Tibble:
    """Convert a pandas DataFrame object to Tibble object"""
    return Tibble(df, copy=False)


@as_tibble.register(DataFrameGroupBy)
def _(df: DataFrameGroupBy) -> TibbleGrouped:
    """Convert a pandas DataFrame object to TibbleGrouped object"""
    return TibbleGrouped.from_groupby(df)


@as_tibble.register(Tibble)
def _(df: Tibble) -> Tibble:
    """Convert a pandas DataFrame object to TibbleGrouped object"""
    return df
