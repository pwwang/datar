"""Port `table` function from r-base"""

from typing import Any, Iterable, Mapping, Tuple, Union, List

import numpy
import pandas
from pandas import Categorical, DataFrame, Series
from pipda import register_func
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.types import (
    ArrayLikeType,
    is_iterable,
    is_scalar,
    is_categorical,
    is_null,
)
from ..core.utils import Array, categorized, fillna_safe

from .na import NA


@register_func(None, context=Context.EVAL)
def table(
    input: Union[ArrayLikeType, Categorical, DataFrame],
    *more_inputs: Any,
    exclude: Any = NA,
    # use_na: str = "no", # TODO
    dnn: Union[str, List[str]] = None,
    # not supported, varname.argname not working with wrappers having
    # different signatures.
    # TODO: varname.argname2() now supports it
    # deparse_level: int = 1
) -> DataFrame:

    """uses the cross-classifying factors to build a contingency table of
    the counts at each combination of factor levels.

    When used with DataFrameGroupBy data, groups are ignored.

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
    elif is_iterable(dnn):
        dnn = list(dnn)
        if len(dnn) == 1:
            dnn = dnn * 2
        dn1, dn2 = dnn[:2]

    if obj1 is obj2:
        obj1 = obj2 = _iterable_excludes(obj1, exclude=exclude)
    else:
        obj1 = _iterable_excludes(obj1, exclude=exclude)
        obj2 = _iterable_excludes(obj2, exclude=exclude)

    kwargs = {"dropna": False}  # type: Mapping[str, Any]
    if dn1:
        kwargs["rownames"] = [dn1]
    if dn2:
        kwargs["colnames"] = [dn2]

    tab = pandas.crosstab(obj1, obj2, **kwargs)
    if obj1 is obj2:
        tab = DataFrame(dict(count=numpy.diag(tab)), index=tab.columns).T
        tab.index.name = dn1
        tab.columns.name = dn2

    return tab


@register_func(None, context=Context.EVAL)
def tabulate(
    bin: Union[ArrayLikeType, Categorical],
    nbins: int = None,
) -> numpy.ndarray:
    """Takes the integer-valued vector `bin` and counts the
    number of times each integer occurs in it.

    Args:
        bin: A numeric vector (of positive integers), or a factor.
        nbins: the number of bins to be used.

    Returns:
        An integer valued ‘integer’ vector (without names).
        There is a bin for each of the values ‘1, ..., nbins’
    """
    from . import as_integer, t

    bin = as_integer(bin, base0_=False, __calling_env=CallingEnvs.REGULAR)
    nbins = max(1, max(bin) if len(bin) > 0 else 0, nbins)
    tabled = table(bin, __calling_env=CallingEnvs.REGULAR)
    tabled = (
        t(tabled, __calling_env=CallingEnvs.REGULAR)
        .reindex(range(1, nbins + 1), fill_value=0)
        .iloc[:, 0]
        .values
    )

    return tabled


def _check_table_inputs(
    input: Any, more_inputs: Any
) -> Tuple[Iterable, Iterable]:
    """Check and clean up `table` inputs"""
    too_many_input_vars_msg = "At most 2 iterables supported for `table`."
    obj1 = obj2 = None
    obj_nvar = 1

    if not isinstance(input, DataFrame):
        obj1 = list(input) if isinstance(input, str) else input
        obj1 = obj1 if is_categorical(obj1) else Array(obj1)
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
            obj2 = Array(list(minput))
        else:
            obj2 = minput if is_categorical(minput) else Array(minput)

    if obj2 is None:
        obj2 = obj1

    return obj1, obj2


def _iterable_excludes(data: Iterable, exclude: Iterable) -> Iterable:
    """Exclude values for categorical data"""
    if is_categorical(data) and exclude is NA:
        return data

    if exclude is None:
        return fillna_safe(data)

    if is_scalar(exclude):
        exclude = [exclude]

    exclude = Array(exclude)

    if is_categorical(data):
        data = categorized(data)
        data = data[~data.isin(exclude)]
        data = data.remove_categories(data.categories.intersection(exclude))

    else:
        data = Series(data)
        data = data[~data.isin(exclude)]

    return data if is_null(exclude).any() else fillna_safe(data)
