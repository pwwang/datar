"""Monkey-patch data frame format to
1. add dtypes next to column names when printing
2. collapse data frames when they are elements of a parent data frame.
"""
from pandas import DataFrame
from pandas.io.formats import format as fmt
from pandas.io.formats.format import (
    DataFrameFormatter,
    GenericArrayFormatter,
    partial,
    List,
    QUOTE_NONE,
    get_option,
    NA,
    NaT,
    np,
    PandasObject,
    extract_array,
    lib,
    notna,
    is_float,
    format_array,
    _trim_zeros_single_float
)
from pandas.io.formats.printing import pprint_thing
from pandas.core.dtypes.common import is_scalar
from pandas.core.dtypes.missing import isna

from .options import add_option

class DatarDataFrameFormatter(DataFrameFormatter):
    """Custom formatter for DataFrame

    """
    def get_strcols(self) -> List[List[str]]:
        """
        Render a DataFrame to a list of columns (as lists of strings).
        """
        strcols = self._get_strcols_without_index()

        if self.index:
            #           dtype
            str_index = [""] + self._get_formatted_index(self.tr_frame)
            strcols.insert(0, str_index)

        return strcols

    def format_col(self, i: int) -> List[str]:
        """Format column, add dtype ahead"""
        frame = self.tr_frame
        formatter = self._get_formatter(i)
        dtype = frame.iloc[:, i].dtype.name

        return [f'<{dtype}>'] + format_array(
            frame.iloc[:, i]._values,
            formatter,
            float_format=self.float_format,
            na_rep=self.na_rep,
            space=self.col_space.get(frame.columns[i]),
            decimal=self.decimal,
            leading_space=self.index,
        )

class DatarGenericArrayFormatter(GenericArrayFormatter):
    """Generic Array Formatter to show DataFrame element in a cell in a
    collpased representation
    """

    def _format_strings(self) -> List[str]: # pragma: no cover
        if self.float_format is None:
            float_format = get_option("display.float_format")
            if float_format is None:
                precision = get_option("display.precision")
                float_format = lambda x: f"{x: .{precision:d}f}"
        else:
            float_format = self.float_format

        if self.formatter is not None:
            formatter = self.formatter
        else:
            quote_strings = (
                self.quoting is not None and self.quoting != QUOTE_NONE
            )
            formatter = partial(
                pprint_thing,
                escape_chars=("\t", "\r", "\n"),
                quote_strings=quote_strings,
            )

        def _format(x):
            if self.na_rep is not None and is_scalar(x) and isna(x):
                try:
                    # try block for np.isnat specifically
                    # determine na_rep if x is None or NaT-like
                    if x is None:
                        return "None"
                    if x is NA:
                        return str(NA)
                    if x is NaT or np.isnat(x):
                        return "NaT"
                except (TypeError, ValueError):
                    # np.isnat only handles datetime or timedelta objects
                    pass
                return self.na_rep
            # Show data frame as collapsed representation
            if isinstance(x, DataFrame):
                return f"<DF {x.shape[0]}x{x.shape[1]}>"
            if isinstance(x, PandasObject):
                return str(x)
            # else:
            # object dtype
            return str(formatter(x))

        vals = extract_array(self.values, extract_numpy=True)

        is_float_type = (
            # pylint: disable=c-extension-no-member
            lib.map_infer(vals, is_float)
            # vals may have 2 or more dimensions
            & np.all(notna(vals), axis=tuple(range(1, len(vals.shape))))
        )
        leading_space = self.leading_space
        if leading_space is None:
            leading_space = is_float_type.any()

        fmt_values = []
        for i, v in enumerate(vals): # pylint: disable=invalid-name
            if not is_float_type[i] and leading_space:
                fmt_values.append(f" {_format(v)}")
            elif is_float_type[i]:
                fmt_values.append(_trim_zeros_single_float(float_format(v)))
            else:
                if leading_space is False:
                    # False specifically, so that the default is
                    # to include a space if we get here.
                    tpl = "{v}"
                else:
                    tpl = " {v}"
                fmt_values.append(tpl.format(v=_format(v)))

        return fmt_values

def _patch(option: bool) -> None:
    """Patch pandas?"""
    if option:
        # monkey-patch the formatter
        fmt.DataFrameFormatter = DatarDataFrameFormatter
        fmt.GenericArrayFormatter = DatarGenericArrayFormatter
    else:
        fmt.DataFrameFormatter = DataFrameFormatter
        fmt.GenericArrayFormatter = GenericArrayFormatter

add_option('frame_format_patch', True, _patch)
