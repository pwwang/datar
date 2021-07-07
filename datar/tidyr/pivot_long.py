"""Pivot data from wide to long

https://github.com/tidyverse/tidyr/blob/HEAD/R/pivot-long.R
"""
import re
from typing import Iterable, Mapping, Callable, Union

import pandas
from pandas import DataFrame
from pandas.core.dtypes.common import is_categorical_dtype
from pipda import register_verb

from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.contexts import Context
from ..core.types import NAType, StringOrIter, Dtype, is_scalar
from ..core.utils import vars_select, apply_dtypes, reconstruct_tibble
from ..core.names import repair_names

from ..base import setdiff, union
from ..dplyr import relocate

from .extract import extract
from .separate import separate

# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=no-value-for-parameter


@register_verb(DataFrame, context=Context.SELECT)
def pivot_longer(
    _data: DataFrame,
    cols: StringOrIter,
    names_to: Union[NAType, str, Iterable[Union[NAType, str]]] = "name",
    names_prefix: str = None,
    names_sep: str = None,
    names_pattern: str = None,
    names_ptypes: Union[Dtype, Mapping[str, Dtype]] = None,
    names_transform: Union[Callable, Mapping[str, Callable]] = None,
    names_repair="check_unique",
    values_to: str = "value",
    values_drop_na: bool = False,
    values_ptypes: Union[Dtype, Mapping[str, Dtype]] = None,
    values_transform: Union[Callable, Mapping[str, Callable]] = None,
    base0_: bool = None,
):
    """ "lengthens" data, increasing the number of rows and
    decreasing the number of columns.

    The row order is a bit different from `tidyr` and `pandas.DataFrame.melt`.
        >>> df = tibble(x=f[1:2], y=f[3:4])
        >>> pivot_longer(df, f[f.x:f.y])
        >>> #    name   value
        >>> # 0  x      1
        >>> # 1  x      2
        >>> # 2  y      3
        >>> # 3  y      4
    But with `tidyr::pivot_longer`, the output will be:
        >>> # # A tibble: 4 x 2
        >>> # name  value
        >>> # <chr> <int>
        >>> # 1 x   1
        >>> # 2 y   3
        >>> # 3 x   2
        >>> # 4 y   4

    Args:
        _data: A data frame to pivot.
        cols: Columns to pivot into longer format.
        names_to: A string specifying the name of the column to create from
            the data stored in the column names of data.
            Can be a character vector, creating multiple columns, if names_sep
            or names_pattern is provided. In this case, there are two special
            values you can take advantage of:
            - `None`/`NA`/`NULL` will discard that component of the name.
            - `.value`/`_value` indicates that component of the name defines
                the name of the column containing the cell values,
                overriding values_to.
            - Different as `tidyr`: With `.value`/`_value`, if there are other
              parts of the names to distinguish the groups, they must be
              captured. For example, use `r'(\\w)_(\\d)'` to match `'a_1'` and
              `['.value', NA]` to discard the suffix, instead of use
              `r'(\\w)_\\d'` to match.
        names_prefix: A regular expression used to remove matching text from
            the start of each variable name.
        names_sep: and
        names_pattern: If names_to contains multiple values,
            these arguments control how the column name is broken up.
            names_sep takes the same specification as separate(), and
            can either be a numeric vector (specifying positions to break on),
            or a single string (specifying a regular expression to split on).
        names_pattern: takes the same specification as extract(),
            a regular expression containing matching groups (()).
        names_ptypes: and
        values_ptypes: A list of column name-prototype pairs.
            A prototype (or ptype for short) is a zero-length vector
            (like integer() or numeric()) that defines the type, class, and
            attributes of a vector. Use these arguments if you want to confirm
            that the created columns are the types that you expect.
            Note that if you want to change (instead of confirm) the types
            of specific columns, you should use names_transform or
            values_transform instead.
        names_transform: and
        values_transform: A list of column name-function pairs.
            Use these arguments if you need to change the types of
            specific columns. For example,
            names_transform = dict(week = as.integer) would convert a
            character variable called week to an integer.
            If not specified, the type of the columns generated from names_to
            will be character, and the type of the variables generated from
            values_to will be the common type of the input columns used to
            generate them.
        names_repair: Not supported yet.
        values_to: A string specifying the name of the column to create from
            the data stored in cell values. If names_to is a character
            containing the special `.value`/`_value` sentinel, this value
            will be ignored, and the name of the value column will be derived
            from part of the existing column names.
        values_drop_na: If TRUE, will drop rows that contain only NAs in
            the value_to column. This effectively converts explicit missing
            values to implicit missing values, and should generally be used
            only when missing values in data were created by its structure.
        names_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair
        base0_: Whether `cols` are 0-based if given by indexes
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The pivoted dataframe.
    """
    rowid_column = "_PIVOT_ROWID_"
    ret = _data.assign(**{rowid_column: range(_data.shape[0])})
    all_columns = ret.columns
    columns = _data.columns[vars_select(_data.columns, cols, base0=base0_)]
    id_columns = all_columns.difference(columns)

    if is_scalar(names_to):
        names_to = [names_to] # type: ignore

    tmp_names_to = []
    # We need to NA/names to be kept for .value pivot
    na_names_to = []
    for i, name in enumerate(names_to):
        if pandas.isnull(name):
            na_name = f"__{DEFAULT_COLUMN_PREFIX}_NA_{i}__"
            na_names_to.append(na_name)
            tmp_names_to.append(na_name)
        elif name == "_value":
            tmp_names_to.append(".value")
        else:
            tmp_names_to.append(name)
    names_to = tmp_names_to

    if len(names_to) > 1 and not names_sep and not names_pattern:
        raise ValueError(
            "If you supply multiple names in `names_to` you must also "
            "supply one of `names_sep` or `names_pattern`."
        )

    if names_sep and names_pattern:
        raise ValueError(
            "Only one of `names_sep` or `names_pattern` should be supplied."
        )

    var_name = "__tmp_names_to__" if names_pattern or names_sep else names_to[0]
    ret = ret.melt(
        id_vars=id_columns,
        # Use the rest columns automatically.
        # Don't specify so that duplicated column names can be used.
        # value_vars=columns,
        var_name=var_name,
        value_name=values_to,
    )
    if names_prefix:
        names_prefix = re.compile(f"^{re.escape(names_prefix)}")
        ret[var_name] = ret[var_name].str.replace(names_prefix, "")

    if all(is_categorical_dtype(_data[col]) for col in columns):
        ret[values_to] = ret[values_to].astype("category")

    if names_pattern:
        ret >>= extract(var_name, into=names_to, regex=names_pattern)

    if names_sep:
        ret >>= separate(var_name, into=names_to, sep=names_sep)
    # extract/separate puts `into` last
    ret >>= relocate(values_to, _after=-1, base0_=True)

    if ".value" in names_to:
        names_to = setdiff(names_to, [".value"])
        index_columns = union(id_columns, names_to)
        names_to = setdiff(names_to, na_names_to)

        # keep the order
        value_columns = pandas.unique(ret[".value"].values)
        ret.set_index(index_columns, inplace=True)
        ret.index = list(ret.index)
        ret2 = ret.pivot(columns=".value", values=values_to).reset_index()
        id_data = DataFrame(ret2["index"].tolist(), columns=index_columns)
        ret = pandas.concat(
            [
                id_data[
                    id_data.columns.difference(na_names_to).difference(
                        [rowid_column]
                    )
                ],
                ret2[value_columns],
            ],
            axis=1,
        )
        values_to = value_columns
    else:
        values_to = [values_to]
        ret.drop(columns=[rowid_column], inplace=True)

    if values_drop_na:
        ret.dropna(subset=values_to, inplace=True)

    names_data = ret[names_to]
    apply_dtypes(names_data, names_ptypes)
    ret[names_to] = names_data

    values_data = ret[values_to]
    apply_dtypes(values_data, values_ptypes)
    ret[values_to] = values_data

    if names_transform:
        for name in names_to:
            if callable(names_transform):
                ret[name] = ret[name].apply(names_transform)
            elif name in names_transform:
                ret[name] = ret[name].apply(names_transform[name])

    if values_transform:
        for name in values_to:
            if callable(values_transform):
                ret[name] = ret[name].apply(values_transform)
            elif name in values_transform:
                ret[name] = ret[name].apply(values_transform[name])

    names = repair_names(ret.columns.tolist(), names_repair, base0_=base0_)
    ret.columns = names

    return reconstruct_tibble(_data, ret)
