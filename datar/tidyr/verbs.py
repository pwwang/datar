"""Verbs from R-tidyr"""

from pandas import DataFrame
from pipda import register_verb, Context

from ..core.utils import select_columns, list_diff

@register_verb(DataFrame, context=Context.SELECT)
def pivot_longer(
        _data,
        cols,
        names_to="name",
        names_prefix=None,
        names_sep=None,
        names_pattern=None,
        names_ptypes=None,
        names_transform=None,
        # names_repair="check_unique",
        values_to="value",
        values_drop_na=False,
        values_ptypes=None,
        values_transform=None
):
    """"lengthens" data, increasing the number of rows and
    decreasing the number of columns.

    Args:
        _data: A data frame to pivot.
        cols: Columns to pivot into longer format.
        names_to: A string specifying the name of the column to create from
            the data stored in the column names of data.
            Can be a character vector, creating multiple columns, if names_sep
            or names_pattern is provided. In this case, there are two special
            values you can take advantage of:
            - None will discard that component of the name.
            - .value indicates that component of the name defines the name of
                the column containing the cell values, overriding values_to.
        names_prefix: A regular expression used to remove matching text from
            the start of each variable name.
        names_sep, names_pattern: If names_to contains multiple values,
            these arguments control how the column name is broken up.
            names_sep takes the same specification as separate(), and
            can either be a numeric vector (specifying positions to break on),
            or a single string (specifying a regular expression to split on).
        names_pattern: takes the same specification as extract(),
            a regular expression containing matching groups (()).
        names_ptypes, values_ptypes: A list of column name-prototype pairs.
            A prototype (or ptype for short) is a zero-length vector
            (like integer() or numeric()) that defines the type, class, and
            attributes of a vector. Use these arguments if you want to confirm
            that the created columns are the types that you expect.
            Note that if you want to change (instead of confirm) the types
            of specific columns, you should use names_transform or
            values_transform instead.
        names_transform, values_transform: A list of column name-function pairs.
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
            containing the special .value sentinel, this value will be ignored,
            and the name of the value column will be derived from part of
            the existing column names.
        values_drop_na: If TRUE, will drop rows that contain only NAs in
            the value_to column. This effectively converts explicit missing
            values to implicit missing values, and should generally be used
            only when missing values in data were created by its structure.

    Returns:
        The pivoted dataframe.
    """
    columns = select_columns(_data.columns, cols)
    id_columns = list_diff(_data.columns, columns)
    var_name = '__tmp_names_to__' if names_pattern or names_sep else names_to
    ret = _data.melt(
        id_vars=id_columns,
        value_vars=columns,
        var_name=var_name,
        value_name=values_to,
    )

    if names_pattern:
        ret[names_to] = ret['__tmp_names_to__'].str.extract(names_pattern)
        ret.drop(['__tmp_names_to__'], axis=1, inplace=True)

    if names_prefix:
        ret[names_to] = ret[names_to].str.replace(names_prefix, '')

    if '.value' in names_to:
        ret2 = ret.pivot(columns='.value', values=values_to)
        rest_columns = list_diff(ret.columns, ['.value', values_to])
        ret2.loc[:, rest_columns] = ret.loc[:, rest_columns]

        ret2_1 = ret2.iloc[:(ret2.shape[0] // 2), ]
        ret2_2 = ret2.iloc[(ret2.shape[0] // 2):, ].reset_index()
        ret = ret2_1.assign(**{col: ret2_2[col] for col in ret2_1.columns
                               if ret2_1[col].isna().all()})

    if values_drop_na:
        ret.dropna(subset=[values_to], inplace=True)
    if names_ptypes:
        for key, ptype in names_ptypes.items():
            ret[key] = ret[key].astype(ptype)
    if values_ptypes:
        for key, ptype in values_ptypes.items():
            ret[key] = ret[key].astype(ptype)
    if names_transform:
        for key, tform in names_transform.items():
            ret[key] = ret[key].apply(tform)
    if values_transform:
        for key, tform in values_transform.items():
            ret[key] = ret[key].apply(tform)

    return ret