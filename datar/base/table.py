"""Port `table` function from r-base"""
import numpy as np
from pipda import register_func

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar, is_categorical_dtype

from ..core.contexts import Context
from ..core.utils import ensure_nparray
from ..core.defaults import NA_REPR

from .factor import _ensure_categorical


def _fillna_safe(data, rep=NA_REPR):
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
        raise ValueError(
            "The value to replace NAs is already present in data."
        )

    if not pd.isnull(data).any():
        return data

    if is_categorical_dtype(data):
        data = _ensure_categorical(data)
        data = data.add_categories(rep)
        return data.fillna(rep)

    # rep may not be the same dtype as data
    return Series(data).fillna(rep).values


@register_func(context=Context.EVAL)
def table(
    input,
    *more_inputs,
    exclude=np.nan,
    # use_na: str = "no", # TODO
    dnn=None,
    # not supported, varname.argname not working with wrappers having
    # different signatures.
    # TODO: varname.argname2() now supports it
    # deparse_level: int = 1
):

    """uses the cross-classifying factors to build a contingency table of
    the counts at each combination of factor levels.

    When used with TibbleGrouped data, groups are ignored.

    Args:
        input: and
        *more_inputs: one or more objects which can be interpreted as factors
            Only 1 or 2 variables allowed currently.
            If obj or elements of objs is a DataFrame, each column is counted
            as a variable.
        exclude: levels to remove for all factors
            By default exclude NA. Set to None to not exclude anything.
        dnn: the names to be given to the dimensions in the result.

    Returns:
        A contingency table (DataFrame)
    """
    obj1, obj2 = _check_table_inputs(input, more_inputs)

    dn1 = dn2 = None
    if isinstance(dnn, str):
        dn1 = dn2 = dnn
    elif not is_scalar(dnn):
        dnn = list(dnn)
        if len(dnn) == 1:
            dnn = dnn * 2
        dn1, dn2 = dnn[:2]

    if obj1 is obj2:
        obj1 = obj2 = _iterable_excludes(obj1, exclude=exclude)
    else:
        obj1 = _iterable_excludes(obj1, exclude=exclude)
        obj2 = _iterable_excludes(obj2, exclude=exclude)

    kwargs = {"dropna": False}
    if dn1:
        kwargs["rownames"] = [dn1]
    if dn2:
        kwargs["colnames"] = [dn2]

    tab = pd.crosstab(obj1, obj2, **kwargs)
    if obj1 is obj2:
        tab = DataFrame(dict(count=np.diag(tab)), index=tab.columns).T
        tab.index.name = dn1
        tab.columns.name = dn2

    return tab


@register_func(context=Context.EVAL)
def tabulate(bin, nbins=None):
    """Takes the integer-valued vector `bin` and counts the
    number of times each integer occurs in it.

    Args:
        bin: A numeric vector (of positive integers), or a factor.
            When bin is a factor, for example `factor(list("abc"))`,
            it is recoded as `[1, 2, 3]` instead of `[0, 1, 2]`
        nbins: the number of bins to be used.

    Returns:
        An integer valued ‘integer’ vector (without names).
        There is a bin for each of the values ‘1, ..., nbins’
    """
    from .casting import as_integer

    is_cat = is_categorical_dtype(bin)
    bin = as_integer(bin)
    if is_cat:
        bin = bin + 1

    nbins = max(
        1,
        bin if is_scalar(bin) else 0 if len(bin) == 0 else max(bin),
        0 if nbins is None else nbins,
    )
    tabled = table(bin)
    tabled = (
        tabled.T
        .reindex(range(1, nbins + 1), fill_value=0)
        .iloc[:, 0]
        .values
    )

    return tabled


def _check_table_inputs(input, more_inputs):
    """Check and clean up `table` inputs"""
    too_many_input_vars_msg = "At most 2 iterables supported for `table`."
    obj1 = obj2 = None
    obj_nvar = 1

    if not isinstance(input, DataFrame):
        obj1 = list(input) if isinstance(input, str) else input
        obj1 = obj1 if is_categorical_dtype(obj1) else ensure_nparray(obj1)
        obj_nvar = 1
    elif input.shape[1] == 0:
        raise ValueError("`input` of `table` has no columns.")
    elif input.shape[1] == 1:
        obj1 = input.iloc[:, 0]
        obj_nvar = 1
    elif input.shape[1] == 2:
        obj1 = input.iloc[:, 0]
        obj2 = input.iloc[:, 1]
        obj_nvar = 2
    else:
        raise ValueError(too_many_input_vars_msg)

    if obj_nvar == 2 and more_inputs:
        raise ValueError(too_many_input_vars_msg)

    if more_inputs:
        if len(more_inputs) > 1:
            raise ValueError(too_many_input_vars_msg)

        minput = more_inputs[0]
        if isinstance(minput, DataFrame):
            if minput.shape[1] > 1:
                raise ValueError(too_many_input_vars_msg)
            if minput.shape[1] == 0:
                raise ValueError("`*more_inputs` of `table` has no columns.")
            obj2 = minput.iloc[:, 0]
        elif isinstance(minput, str):
            obj2 = np.array(list(minput))
        else:
            obj2 = minput if is_categorical_dtype(minput) else np.array(minput)

    if obj2 is None:
        obj2 = obj1

    return obj1, obj2


def _iterable_excludes(data, exclude):
    """Exclude values for categorical data"""
    if is_categorical_dtype(data) and pd.isnull(exclude):
        return data

    if exclude is None:
        return _fillna_safe(data)

    if is_scalar(exclude):
        exclude = [exclude]

    exclude = ensure_nparray(exclude)

    if is_categorical_dtype(data):
        data = _ensure_categorical(data)
        data = data[~data.isin(exclude)]
        data = data.remove_categories(data.categories.intersection(exclude))

    else:
        data = Series(data)
        data = data[~data.isin(exclude)]

    return data if pd.isnull(exclude).any() else _fillna_safe(data)
