"""Core utilities"""

import logging
import inspect
from functools import singledispatch
from copy import deepcopy
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Mapping,
    Sequence,
    Union,
    Tuple,
)

import numpy
from numpy import array as Array

import pandas
from pandas import Categorical, DataFrame, Series
from pipda import register_func
from pipda.symbolic import Reference

from .exceptions import (
    ColumnNotExistingError,
    DataUnrecyclable,
    NameNonUniqueError,
)
from .contexts import Context
from .types import (
    StringOrIter,
    Dtype,
    is_iterable,
    is_scalar,
    is_categorical,
    is_null,
)
from .defaults import DEFAULT_COLUMN_PREFIX, NA_REPR

# logger
logger = logging.getLogger("datar")  # pylint: disable=invalid-name
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()  # pylint: disable=invalid-name
stream_handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s][%(name)s][%(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(stream_handler)


def vars_select(
    all_columns: Iterable[str],
    *columns: Any,
    raise_nonexists: bool = True,
    base0: bool = None,
) -> List[int]:
    # TODO: support selecting data-frame columns
    """Select columns

    Args:
        all_columns: The column pool to select
        *columns: arguments to select from the pool
        raise_nonexist: Whether raise exception when column not exists
            in the pool
        base0: Whether indexes are 0-based if columns are selected by indexes.
            If not given, will use `datar.base.get_option('index.base.0')`

    Returns:
        The selected indexes for columns

    Raises:
        ColumnNotExistingError: When the column does not exist in the pool
            and raise_nonexists is True.
    """
    from .collections import Collection
    from ..base import unique

    columns = [
        column.name if isinstance(column, Series) else column
        for column in columns
    ]
    selected = Collection(*columns, pool=list(all_columns), base0=base0)
    if raise_nonexists and selected.unmatched and selected.unmatched != {None}:
        raise ColumnNotExistingError(
            f"Columns `{selected.unmatched}` do not exist."
        )
    return unique(selected).astype(int)


def recycle_value(
    value: Any, size: int, name: str = None
) -> Union[DataFrame, numpy.ndarray]:
    """Recycle a value based on a dataframe

    Args:
        value: The value to be recycled
        size: The size to recycle to
        name: The name to show in the error if failed to recycle

    Returns:
        The recycled value
    """
    # TODO: follow base R's recycling rule? i.e. size 2 -> 4
    from ..base import NA

    if is_scalar(value):
        value = [value]

    length = len(value)

    if length not in (0, 1, size):
        name = "value" if not name else f"`{name}`"
        expect = "1" if size == 1 else f"(1, {size})"
        raise DataUnrecyclable(
            f"Cannot recycle {name} to size {size}, "
            f"expect {expect}, got {length}."
        )

    if isinstance(value, DataFrame):
        if length == size == 0:
            return DataFrame(columns=value.columns)
        if length == 0:
            value = DataFrame([[NA] * value.shape[1]], columns=value.columns)
        if length == 1 and size > length:
            return value.iloc[[0] * size, :].reset_index(drop=True)
        return value

    cats = categorized(value).categories if is_categorical(value) else None

    if length == size == 0:
        return [] if cats is None else Categorical([], categories=cats)

    if length == 0:
        value = [NA]

    if isinstance(value, Series):
        # try to keep Series class
        # some operators can only do with it or with it correctly
        # For example:
        # Series([True, True]) & Series([False, NA]) -> [False, Fa.se]
        # But with numpy.array, it raises error, since NA is a float
        if length == 1 and size > length:
            value = value.iloc[[0] * size].reset_index(drop=True)
        return value

    if isinstance(value, tuple):
        value = list(value)

    # dtype = getattr(value, 'dtype', None)
    if length == 1 and size > length:
        value = list(value) * size

    if cats is not None:
        return Categorical(value, categories=cats)

    is_elem_iter = any(is_iterable(val) for val in value)
    if is_elem_iter:
        # without dtype: VisibleDeprecationWarning
        # return Array(value, dtype=object)
        # The above does not keep [DataFrame()] structure
        return value

    # Avoid numpy.nan to be converted into 'nan' when other elements are string
    out = Array(value)
    if numpy.issubdtype(out.dtype, numpy.str_) and is_null(value).any():
        return Array(value, dtype=object)
    return out


def recycle_df(
    df: DataFrame,
    value: Any,
    df_name: str = None,
    value_name: str = None,
) -> Tuple[DataFrame, Any]:
    """Recycle the dataframe based on value"""
    if length_of(df) == 1:
        df = recycle_value(df, length_of(value), df_name)
    value = recycle_value(value, length_of(df), value_name)
    return df, value


def categorized(data: Any) -> Any:
    """Get the Categorical object"""
    if not is_categorical(data):
        return data
    if isinstance(data, Series):
        return data.values
    return data


@singledispatch
def to_df(data: Any, name: str = None) -> DataFrame:
    """Convert an object to a data frame"""
    if is_scalar(data):
        data = [data]

    if name is None:
        return DataFrame(data)

    return DataFrame({name: data})


@to_df.register(numpy.ndarray)
def _(data: numpy.ndarray, name: StringOrIter = None) -> DataFrame:
    if name is not None and is_scalar(name):
        name = [name]

    if len(data.shape) == 1:
        return (
            DataFrame(data, columns=name)
            if name is not None
            else DataFrame(data)
        )

    ncols = data.shape[1]
    if name is not None and len(name) == ncols:
        return DataFrame(data, columns=name)
    # ignore the name
    return DataFrame(data)


@to_df.register(DataFrame)
def _(data: DataFrame, name: str = None) -> DataFrame:
    if name is None:
        return data
    return DataFrame({f"{name}${col}": data[col] for col in data.columns})


@to_df.register(Series)
def _(data: Series, name: str = None) -> DataFrame:
    name = name or data.name
    return data.to_frame(name=name)


# @to_df.register(SeriesGroupBy)
# def _(data: SeriesGroupBy, name: str = None) -> DataFrame:
#     name = name or data.obj.name
#     return data.obj.to_frame(name=name).groupby(data.grouper, dropna=False)


def check_column_uniqueness(df: DataFrame, msg: str = None) -> None:
    """Check if column names are unique of a dataframe"""
    uniq = set()
    for col in df.columns:
        if col not in uniq:
            uniq.add(col)
        else:
            msg = msg or "Name is not unique"
            raise NameNonUniqueError(f"{msg}: {col}")


def dict_insert_at(
    container: Mapping[str, Any],
    poskeys: Sequence[str],
    value: Mapping[str, Any],
    remove: bool = False,
) -> Mapping[str, Any]:
    """Insert value to a certain position of a dict"""
    ret_items = []  # type: List[Tuple[str, Any]]
    ret_items_append = ret_items.append
    matched = False
    for key, val in container.items():
        if key == poskeys[0]:
            matched = True
            if not remove:
                ret_items_append((key, val))
            ret_items.extend(value.items())
        elif matched and key in poskeys:
            if not remove:
                ret_items_append((key, val))
        elif matched and key not in poskeys:
            matched = False
            ret_items_append((key, val))
        else:
            ret_items_append((key, val))

    return dict(ret_items)


def name_mutatable_args(
    *args: Union[Series, DataFrame, Mapping[str, Any]],
    **kwargs: Any,
) -> Mapping[str, Any]:
    """Convert all mutatable arguments to named mappings, which can be easier
    to mutate later on.

    If there are Expression objects, keep it. So if an objects have multiple
    names and it's built by an Expression, then the name might get lost here.

    Examples:

        >>> s = Series([1], name='a')
        >>> name_mutatable_args(s, b=2)
        >>> # {'a': s, 'b': 2}
        >>> df = DataFrame({'x': [3], 'y': [4]})
        >>> name_mutatable_args(df)
        >>> # {'x': [3], 'y': [4]}
        >>> name_mutatable_args(d=df)
        >>> # {'d$x': [3], 'd$y': [4]}
    """
    # order kept
    ret = {}  # type: dict

    for i, arg in enumerate(args):
        if isinstance(arg, Series):
            ret[arg.name] = arg
        elif isinstance(arg, dict):
            ret.update(arg)
        elif isinstance(arg, DataFrame):
            ret.update(arg.to_dict("series"))
        elif isinstance(arg, Reference):
            ret[arg._pipda_ref] = arg
        else:
            ret[f"{DEFAULT_COLUMN_PREFIX}{i}"] = arg

    for key, val in kwargs.items():
        if isinstance(val, DataFrame):
            val = val.to_dict("series")

        if isinstance(val, dict):
            existing_keys = [
                ret_key
                for ret_key in ret
                if ret_key == key or ret_key.startswith(f"{key}$")
            ]
            if existing_keys:
                ret = dict_insert_at( # type: ignore
                    ret, existing_keys, {key: val}, remove=True
                )
            else:
                for dkey, dval in val.items():
                    ret[f"{key}${dkey}"] = dval
        else:
            ret[key] = val
    return ret


def arg_match(
    arg: Any, argname: str, values: Iterable[Any], errmsg: str = None
) -> Any:
    """Make sure arg is in one of the values.

    Mimics `rlang::arg_match`.
    """
    if not errmsg:
        values = list(values)
        errmsg = f"`{argname}` must be one of {values}."
    if arg not in values:
        raise ValueError(errmsg)
    return arg


def copy_attrs(df1: DataFrame, df2: DataFrame, deep: bool = True) -> None:
    """Copy attrs from df2 to df1"""
    for key, val in df2.attrs.items():
        if key.startswith("_"):
            continue
        df1.attrs[key] = deepcopy(val) if deep else val


def nargs(fun: Callable) -> int:
    """Get the number of arguments of a function"""
    return len(inspect.signature(fun).parameters)


def position_at(
    pos: int, length: int, base0: bool = None, raise_exc: bool = True
) -> int:
    """Get the 0-based position right at the given pos

    When `pos` is negative, it acts like 0-based, meaning `-1` will anyway
    represent the last position regardless of `base0`

    Args:
        pos: The given position
        length: The length of the pool
        base0: Whether the given `pos` is 0-based
        raise_exc: Raise error if `pos` is out of range?

    Returns:
        The 0-based position
    """
    from .collections import Collection

    coll = Collection(pos, pool=length, base0=base0)
    if raise_exc and coll.error:
        # pylint: disable=raising-bad-type
        raise coll.error
    return coll[0]


def position_after(pos: int, length: int, base0: bool = None) -> int:
    """Get the 0-based position right at the given pos

    Args:
        pos: The given position
        length: The length of the pool

    Returns:
        The position before the given position
    """
    base0 = get_option("index.base.0", base0)
    # after 0 with 1-based, should insert to first column
    if not base0 and pos == 0:
        return 0

    return position_at(pos, length, base0) + 1


def get_option(key: str, value: Any = None) -> Any:
    """Get the option with key.

    This is for interal use mostly.

    This is a shortcut for:
    >>> if value is not None:
    >>>     return value
    >>> from datar.base import get_option
    >>> return get_option(key)
    """
    if value is not None:
        return value
    from ..base import get_option as get_option_

    return get_option_(key)


def apply_dtypes(
    df: DataFrame, dtypes: Union[bool, Dtype, Mapping[str, Dtype]]
) -> None:
    """Apply dtypes to data frame"""
    if dtypes is None or dtypes is False:
        return

    if dtypes is True:
        inferred = df.convert_dtypes()
        for col in df:
            df[col] = inferred[col]
        return

    if not isinstance(dtypes, dict):
        dtypes = dict(zip(df.columns, [dtypes] * df.shape[1])) # type: ignore

    for column, dtype in dtypes.items():
        if column in df:
            df[column] = df[column].astype(dtype)
        else:
            for col in df:
                if col.startswith(f"{column}$"):
                    df[col] = df[col].astype(dtype)


def keep_column_order(df: DataFrame, order: Iterable[str]):
    """Keep the order of columns as given `order`

    We cannot do `df[order]` directly, since `df` may have nested df columns.
    """
    out_columns = []
    for col in order:
        if col in df:
            out_columns.append(col)
        else:
            out_columns.extend(
                (dfcol for dfcol in df.columns if dfcol.startswith(f"{col}$"))
            )
    if set(out_columns) != set(df.columns):
        raise ValueError("Given `order` does not select all columns.")

    return df[out_columns]


def reconstruct_tibble(
    input: DataFrame,  # pylint: disable=redefined-builtin
    output: DataFrame,
    ungrouped_vars: List[str] = None,
    keep_rowwise: bool = False,
) -> DataFrame:
    """Reconstruct the output dataframe based on input dataframe

    Args:
        input: The input data frame
        output: The output data frame
        ungrouped_vars: Variables to exclude from grouping
        keep_rowwise: Whether rowwise structure should be kept

    Return:
        The reconstructed dataframe.
    """
    from ..base import setdiff, intersect
    from ..dplyr import group_vars, group_by_drop_default
    from .grouped import DataFrameGroupBy, DataFrameRowwise

    if ungrouped_vars is None:
        ungrouped_vars = []
    old_groups = group_vars(input)
    new_groups = intersect(setdiff(old_groups, ungrouped_vars), output.columns)

    if isinstance(input, DataFrameRowwise):
        out = (
            DataFrameRowwise(
                output,
                _group_vars=new_groups,
                _group_drop=group_by_drop_default(input),
            )
            if keep_rowwise
            else output
        )
    elif isinstance(input, DataFrameGroupBy) and len(new_groups) > 0:
        out = DataFrameGroupBy(
            output,
            _group_vars=new_groups,
            _group_drop=group_by_drop_default(input),
        )
    else:
        out = output

    copy_attrs(out, input)
    return out


def df_getitem(df: DataFrame, ref: Any) -> Union[DataFrame, numpy.ndarray]:
    """Select columns from a data frame

    If the column is a data frame, select that data frame.
    """
    try:
        return df[ref]
    except KeyError:
        cols = [col for col in df.columns if col.startswith(f"{ref}$")]
        if not cols:
            raise KeyError(ref) from None
        ret = df.loc[:, cols]
        ret.columns = [col[len(ref) + 1 :] for col in cols]
        return ret


def df_setitem(
    df: DataFrame, name: str, value: Any, allow_dups: bool = False
) -> DataFrame:
    """Assign an item to a dataframe

    Args:
        df: The data frame
        name: The name of the item
        value: The value to insert
        allow_dups: Allow duplicated names

    Returns:
        df itself or a merged df
    """
    value = recycle_value(value, df.shape[0])
    if isinstance(value, DataFrame):
        # nested df
        value.columns = [f"{name}${col}" for col in value.columns]

        if allow_dups:
            return pandas.concat([df, value], axis=1)

        for col in value.columns:
            df[col] = value[col]
        return df

    if isinstance(value, numpy.ndarray) and value.ndim > 1:
        # keep the list structure
        # otherwise, keep array to have dtype kept
        value = value.tolist()

    # drop the index/match the index of df
    if isinstance(value, Series):
        value = value.values

    # tuple turned into list in recycle_value
    # if isinstance(value, tuple):
    #     # ('A', array([1,2,3]))
    #     # VisibleDeprecationWarning
    #     value = list(value)

    if not allow_dups:
        df[name] = value

    else:
        df.insert(df.shape[1], name, value, allow_duplicates=True)

    return df


def fillna_safe(data: Iterable, rep: Any = NA_REPR) -> Iterable:
    """Safely replace NA in data, as we can't just fillna for
    a Categorical data directly.

    Args:
        data: The data to fill NAs
        rep: The replacement for NAs

    Returns:
        Data itself if it has no NAs.
        For Categorical data, `rep` will be added to categories
        Otherwise a Array object with NAs replaced with `rep` is returned with
        dtype object.

    Raises:
        ValueError: when `rep` exists in data
    """
    # elementwise comparison failed; returning scalar instead
    # if rep in data:
    if rep in list(data):
        raise ValueError("The value to replace NAs is already present in data.")

    if not is_null(data).any():
        return data

    if is_categorical(data):
        data = categorized(data)
        data = data.add_categories(rep)
        return data.fillna(rep)

    # rep may not be the same dtype as data
    return Series(data).fillna(rep).values


def na_if_safe(
    data: Iterable, value: str = NA_REPR, dtype: Dtype = None
) -> Iterable:
    """Replace value with NA

    Args:
        data: The data to replace value with NA
        value: The value to match
        dtype: Convert the data to dtype after replacement
            If data is Categorical, this is ignored

    Return:
        Array or categorical
    """
    if is_categorical(data):
        # ignore dtype
        data = categorized(data)
        # value to NA, value in categories removed
        return data.replace(value, None)

    from ..base import NA

    out = Series(data).replace(value, NA)
    return out if dtype is None else out.astype(dtype)


def length_of(x: Any) -> int:
    """Get the length of a value, scalar gets 1"""
    if is_scalar(x):
        return 1

    return len(x)


def dedup_name(name: str, all_names: Iterable[str]):
    """Check if a name is a duplicated name in all_names,
    return the deduplicated name.

    In other to support duplicated keyword arguments in R:
        >>> df %>% mutate(a=1, a=a*2)

    Now you can to it with datar:
        >>> df >> mutate(a_=1, a=f.a*2)

    Args:
        name: The name to deduplicate
        all_names: all names

    Returns:
        The deduplicated name
    """
    if not name.endswith("_") or name[:-1] not in all_names:
        return name

    # now determine whehter the real name is name[:-1]
    # because the name could be "a__" ("a_" is for sure in all_names)
    # we need to check if "a" is also in all_names
    # otherwise, the realname is "a_"
    name = name[:-1]
    while name.endswith("_") and name[:-1] in all_names:
        name = name[:-1]
    return name


def register_numpy_func_x(
    name: str,
    np_name: str,
    trans_in: Callable = None,
    trans_out: Callable = None,
    doc: str = "",
) -> Callable:
    """Register numpy function with single argument x

    Args:
        name: The name of the function to be returned
        np_name: The name of the function from numpy
        trans_in: Transformation for input before sending to numpy function
        trans_out: Transformation for output
        doc: The docstring for the function

    Returns:
        The registered function
    """
    func = getattr(numpy, np_name)

    @register_func(None, context=Context.EVAL)
    def _func(x: Any) -> Any:
        """Registered function from numpy"""
        if trans_in:
            x = trans_in(x)

        out = func(x)
        if trans_out:  # pragma: no cover
            out = trans_out(out)
        return out

    _func.__name__ = name
    _func.__doc__ = doc
    return _func
