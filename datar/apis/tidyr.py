from __future__ import annotations as _
from typing import Any, Callable as _Callable, Mapping as _Mapping

from pipda import (
    register_verb as _register_verb,
    register_func as _register_func,
)

from ..core.utils import (
    NotImplementedByCurrentBackendError as _NotImplementedByCurrentBackendError,
)
from .base import expand_grid  # noqa: F401


@_register_func(pipeable=True, dispatchable=True)
def full_seq(x, period, tol=1e-6) -> Any:
    """Create the full sequence of values in a vector

    Args:
        x: A numeric vector.
        period: Gap between each observation. The existing data will be
            checked to ensure that it is actually of this periodicity.
        tol: Numerical tolerance for checking periodicity.

    Returns:
        The full sequence
    """
    raise _NotImplementedByCurrentBackendError("full_seq", x)


@_register_verb()
def chop(
    data,
    cols=None,
) -> Any:
    """Makes data frame shorter by converting rows within each group
    into list-columns.

    Args:
        data: A data frame
        cols: Columns to chop

    Returns:
        Data frame with selected columns chopped
    """
    raise _NotImplementedByCurrentBackendError("chop", data)


@_register_verb()
def unchop(
    data,
    cols=None,
    keep_empty: bool = False,
    dtypes=None,
) -> Any:
    """Makes df longer by expanding list-columns so that each element
    of the list-column gets its own row in the output.

    See https://tidyr.tidyverse.org/reference/chop.html

    Recycling size-1 elements might be different from `tidyr`
        >>> df = tibble(x=[1, [2,3]], y=[[2,3], 1])
        >>> df >> unchop([f.x, f.y])
        >>> # tibble(x=[1,2,3], y=[2,3,1])
        >>> # instead of following in tidyr
        >>> # tibble(x=[1,1,2,3], y=[2,3,1,1])

    Args:
        data: A data frame.
        cols: Columns to unchop.
        keep_empty: By default, you get one row of output for each element
            of the list your unchopping/unnesting.
            This means that if there's a size-0 element
            (like NULL or an empty data frame), that entire row will be
            dropped from the output.
            If you want to preserve all rows, use `keep_empty` = `True` to
            replace size-0 elements with a single row of missing values.
        dtypes: Providing the dtypes for the output columns.
            Could be a single dtype, which will be applied to all columns, or
            a dictionary of dtypes with keys for the columns and values the
            dtypes.
            For nested data frames, we need to specify `col$a` as key. If `col`
            is used as key, all columns of the nested data frames will be casted
            into that dtype.

    Returns:
        A data frame with selected columns unchopped.
    """
    raise _NotImplementedByCurrentBackendError("unchop", data)


@_register_verb()
def nest(
    _data,
    _names_sep: str = None,
    **cols: str | int,
) -> Any:
    """Nesting creates a list-column of data frames

    Args:
        _data: A data frame
        **cols: Columns to nest
        _names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `_names_sep`.

    Returns:
        Nested data frame.
    """
    raise _NotImplementedByCurrentBackendError("nest", _data)


@_register_verb()
def unnest(
    data,
    *cols: str | int,
    keep_empty: bool = False,
    dtypes=None,
    names_sep: str = None,
    names_repair: str | _Callable = "check_unique",
) -> Any:
    """Flattens list-column of data frames back out into regular columns.

    Args:
        data: A data frame to flatten.
        *cols: Columns to unnest.
        keep_empty: By default, you get one row of output for each element
            of the list your unchopping/unnesting.
            This means that if there's a size-0 element
            (like NULL or an empty data frame), that entire row will be
            dropped from the output.
            If you want to preserve all rows, use `keep_empty` = `True` to
            replace size-0 elements with a single row of missing values.
        dtypes: Providing the dtypes for the output columns.
            Could be a single dtype, which will be applied to all columns, or
            a dictionary of dtypes with keys for the columns and values the
            dtypes.
        names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `names_sep`.
        names_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        Data frame with selected columns unnested.
    """
    raise _NotImplementedByCurrentBackendError("unnest", data)


@_register_verb()
def pack(
    _data,
    _names_sep: str = None,
    **cols: str | int,
) -> Any:
    """Makes df narrow by collapsing a set of columns into a single df-column.

    Args:
        _data: A data frame
        **cols: Columns to pack
        _names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `_names_sep`.
    """
    raise _NotImplementedByCurrentBackendError("pack", _data)


@_register_verb()
def unpack(
    data,
    cols,
    names_sep: str = None,
    names_repair: str | _Callable = "check_unique",
) -> Any:
    """Makes df wider by expanding df-columns back out into individual columns.

    For empty columns, the column is kept asis, instead of removing it.

    Args:
        data: A data frame
        cols: Columns to unpack
        names_sep: If `None`, the default, the names will be left as is.
            Inner names will come from the former outer names
            If a string, the inner and outer names will be used together.
            The names of the new outer columns will be formed by pasting
            together the outer and the inner column names, separated by
            `_names_sep`.
        name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        Data frame with given columns unpacked.
    """
    raise _NotImplementedByCurrentBackendError("unpack", data)


@_register_verb()
def expand(
    data,
    *args,
    _name_repair: str | _Callable = "check_unique",
    **kwargs,
) -> Any:
    """Generates all combination of variables found in a dataset.

    Args:
        data: A data frame
        *args: and,
        **kwargs: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with all combination of variables.
    """
    raise _NotImplementedByCurrentBackendError("expand", data)


@_register_func(dispatchable=True)
def nesting(
    *args,
    _name_repair: str | _Callable = "check_unique",
    **kwargs,
) -> Any:
    """A helper that only finds combinations already present in the data.

    Args:
        *args: and,
        **kwargs: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with all combinations in data.
    """
    raise _NotImplementedByCurrentBackendError("nesting")


@_register_func(dispatchable=True)
def crossing(
    *args,
    _name_repair: str | _Callable = "check_unique",
    **kwargs,
) -> Any:
    """A wrapper around `expand_grid()` that de-duplicates and sorts its inputs

    When values are not specified by literal `list`, they will be sorted.

    Args:
        *args: and,
        **kwargs: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with values deduplicated and sorted.
    """
    raise _NotImplementedByCurrentBackendError("crossing")


@_register_verb()
def complete(
    data,
    *args,
    fill=None,
    explict: bool = True,
) -> Any:
    """Turns implicit missing values into explicit missing values.

    Args:
        data: A data frame
        *args: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        fill: A named list that for each variable supplies a single value
            to use instead of NA for missing combinations.
        explict: Should both implicit (newly created) and explicit
            (pre-existing) missing values be filled by fill? By default,
            this is TRUE, but if set to FALSE this will limit the fill to only
            implicit missing values.

    Returns:
        Data frame with missing values completed
    """
    raise _NotImplementedByCurrentBackendError("complete", data)


@_register_verb()
def drop_na(
    _data,
    *columns: str,
    _how: str = "any",
) -> Any:
    """Drop rows containing missing values

    See https://tidyr.tidyverse.org/reference/drop_na.html

    Args:
        data: A data frame.
        *columns: Columns to inspect for missing values.
        _how: How to select the rows to drop
            - all: All columns of `columns` to be `NA`s
            - any: Any columns of `columns` to be `NA`s
            (tidyr doesn't support this argument)

    Returns:
        Dataframe with rows with NAs dropped and indexes dropped
    """
    raise _NotImplementedByCurrentBackendError("drop_na", _data)


@_register_verb()
def extract(
    data,
    col: str | int,
    into,
    regex: str = r"(\w+)",
    remove: bool = True,
    convert=False,
) -> Any:
    """Given a regular expression with capturing groups, extract() turns each
    group into a new column. If the groups don't match, or the input is NA,
    the output will be NA.

    See https://tidyr.tidyverse.org/reference/extract.html

    Args:
        data: The dataframe
        col: Column name or position.
        into: Names of new variables to create as character vector.
            Use None to omit the variable in the output.
        regex: a regular expression used to extract the desired values.
            There should be one group (defined by ()) for each element of into.
        remove: If TRUE, remove input column from output data frame.
        convert: The universal type for the extracted columns or a dict for
            individual ones

    Returns:
        Dataframe with extracted columns.
    """
    raise _NotImplementedByCurrentBackendError("extract", data)


@_register_verb()
def fill(
    _data,
    *columns: str | int,
    _direction: str = "down",
) -> Any:
    """Fills missing values in selected columns using the next or
    previous entry.

    See https://tidyr.tidyverse.org/reference/fill.html

    Args:
        _data: A dataframe
        *columns: Columns to fill
        _direction: Direction in which to fill missing values.
            Currently either "down" (the default), "up",
            "downup" (i.e. first down and then up) or
            "updown" (first up and then down).

    Returns:
        The dataframe with NAs being replaced.
    """
    raise _NotImplementedByCurrentBackendError("fill", _data)


@_register_verb()
def pivot_longer(
    _data,
    cols,
    names_to="name",
    names_prefix: str = None,
    names_sep: str = None,
    names_pattern: str = None,
    names_dtypes=None,
    names_transform: _Callable | _Mapping[str, _Callable] = None,
    names_repair="check_unique",
    values_to: str = "value",
    values_drop_na: bool = False,
    values_dtypes=None,
    values_transform: _Callable | _Mapping[str, _Callable] = None,
) -> Any:
    """ "lengthens" data, increasing the number of rows and
    decreasing the number of columns.

    The row order is a bit different from `tidyr` and `pandas.DataFrame.melt`.
        >>> df = tibble(x=c[1:2], y=c[3:4])
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
        names_dtypes: and
        values_dtypes: A list of column name-prototype pairs.
            A prototype (or dtypes for short) is a zero-length vector
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

    Returns:
        The pivoted dataframe.
    """
    raise _NotImplementedByCurrentBackendError("pivot_longer", _data)


@_register_verb()
def pivot_wider(
    _data,
    id_cols=None,
    names_from="name",
    names_prefix: str = "",
    names_sep: str = "_",
    names_glue: str = None,
    names_sort: bool = False,
    # names_repair: str = "check_unique", # todo
    values_from="value",
    values_fill=None,
    values_fn: _Callable | _Mapping[str, _Callable] = None,
) -> Any:
    """ "widens" data, increasing the number of columns and decreasing
    the number of rows.

    Args:
        _data: A data frame to pivot.
        id_cols: A set of columns that uniquely identifies each observation.
            Defaults to all columns in data except for the columns specified
            in names_from and values_from.
        names_from: and
        values_from: A pair of arguments describing which column
            (or columns) to get the name of the output column (names_from),
            and which column (or columns) to get the cell values from
            (values_from).
        names_prefix: String added to the start of every variable name.
        names_sep: If names_from or values_from contains multiple variables,
            this will be used to join their values together into a single
            string to use as a column name.
        names_glue: Instead of names_sep and names_prefix, you can supply
            a glue specification that uses the names_from columns
            (and special _value) to create custom column names.
        names_sort: Should the column names be sorted? If FALSE, the default,
            column names are ordered by first appearance.
        names_repair: todo
        values_fill: Optionally, a (scalar) value that specifies what
            each value should be filled in with when missing.
        values_fn: Optionally, a function applied to the value in each cell
            in the output. You will typically use this when the combination
            of `id_cols` and value column does not uniquely identify
            an observation.
            This can be a dict you want to apply different aggregations to
            different value columns.
            If not specified, will be `numpy.mean`

    Returns:
        The pivoted dataframe.
    """
    raise _NotImplementedByCurrentBackendError("pivot_wider", _data)


@_register_verb()
def separate(
    data,
    col: int | str,
    into,
    sep: int | str = r"[^0-9A-Za-z]+",
    remove: bool = True,
    convert=False,
    extra: str = "warn",
    fill: str = "warn",
) -> Any:
    """Given either a regular expression or a vector of character positions,
    turns a single character column into multiple columns.

    Args:
        data: The dataframe
        col: Column name or position.
        into: Names of new variables to create as character vector.
            Use `None`/`NA`/`NULL` to omit the variable in the output.
        sep: Separator between columns.
            If str, `sep` is interpreted as a regular expression.
            The default value is a regular expression that matches
            any sequence of non-alphanumeric values.
            If int, `sep` is interpreted as character positions to split at.
        remove: If TRUE, remove input column from output data frame.
        convert: The universal type for the extracted columns or a dict for
            individual ones
            Note that when given `TRUE`, `DataFrame.convert_dtypes()` is called,
            but it will not convert `str` to other types
            (For example, `'1'` to `1`). You have to specify the dtype yourself.
        extra: If sep is a character vector, this controls what happens when
            there are too many pieces. There are three valid options:
            - "warn" (the default): emit a warning and drop extra values.
            - "drop": drop any extra values without a warning.
            - "merge": only splits at most length(into) times
        fill: If sep is a character vector, this controls what happens when
            there are not enough pieces. There are three valid options:
            - "warn" (the default): emit a warning and fill from the right
            - "right": fill with missing values on the right
            - "left": fill with missing values on the left

    Returns:
        Dataframe with separated columns.
    """
    raise _NotImplementedByCurrentBackendError("separate", data)


@_register_verb()
def separate_rows(
    data,
    *columns: str,
    sep: str = r"[^0-9A-Za-z]+",
    convert=False,
) -> Any:
    """Separates the values and places each one in its own row.

    Args:
        data: The dataframe
        *columns: The columns to separate on
        sep: Separator between columns.
        convert: The universal type for the extracted columns or a dict for
            individual ones

    Returns:
        Dataframe with rows separated and repeated.
    """
    raise _NotImplementedByCurrentBackendError("separate_rows", data)


@_register_verb()
def uncount(
    data,
    weights,
    _remove: bool = True,
    _id: str = None,
) -> Any:
    """Duplicating rows according to a weighting variable

    Args:
        data: A data frame
        weights: A vector of weights. Evaluated in the context of data
        _remove: If TRUE, and weights is the name of a column in data,
            then this column is removed.
        _id: Supply a string to create a new variable which gives a
            unique identifier for each created row (0-based).

    Returns:
        dataframe with rows repeated.
    """
    raise _NotImplementedByCurrentBackendError("uncount", data)


@_register_verb()
def unite(
    data,
    col: str,
    *columns: str | int,
    sep: str = "_",
    remove: bool = True,
    na_rm: bool = True,
) -> Any:
    """Unite multiple columns into one by pasting strings together

    Args:
        data: A data frame.
        col: The name of the new column, as a string or symbol.
        *columns: Columns to unite
        sep: Separator to use between values.
        remove: If True, remove input columns from output data frame.
        na_rm: If True, missing values will be remove prior to uniting
            each value.

    Returns:
        The dataframe with selected columns united
    """
    raise _NotImplementedByCurrentBackendError("unite", data)


@_register_verb()
def replace_na(
    data,
    data_or_replace=None,
    replace=None,
) -> Any:
    """Replace NA with a value

    This function can be also used not as a verb. As a function called as
    an argument in a verb, data is passed implicitly. Then one could
    pass data_or_replace as the data to replace.

    Args:
        data: The data piped in
        data_or_replace: When called as argument of a verb, this is the
            data to replace. Otherwise this is the replacement.
        replace: The value to replace with
            Can only be a scalar or dict for data frame.
            So replace NA with a list is not supported yet.

    Returns:
        Corresponding data with NAs replaced
    """
    raise _NotImplementedByCurrentBackendError("replace_na", data)
