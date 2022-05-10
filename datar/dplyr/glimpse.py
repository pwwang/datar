"""Provides glimpse"""
import textwrap
import html
from functools import singledispatch
from shutil import get_terminal_size

from pipda import register_verb

from ..core.tibble import TibbleGrouped, TibbleRowwise
from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.core.groupby import SeriesGroupBy


@singledispatch
def formatter(x):
    """Formatter passed to glimpse to format a single element of a dataframe."""
    return str(x)


@formatter.register(DataFrame)
def _dataframe_formatter(x):
    """Format a dataframe element."""
    return f"<DF {x.shape[0]}x{x.shape[1]}>"


@formatter.register(str)
def _str_formatter(x):
    """Format a string"""
    return repr(x)


class Glimpse:
    """Glimpse class

    Args:
        x: The data to be glimpseed
        width: The width of the output
        formatter: The formatter to use to format data elements
    """
    def __init__(self, x, width, formatter) -> None:
        self.x = x
        self.width = width or get_terminal_size((100, 20)).columns
        self.formatter = formatter
        self.colwidths = (0, 0)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        self._calculate_output_widths()
        return "\n".join(
            (
                "\n".join(self._general()),
                "\n".join(self._variables()),
            )
        )

    def _repr_html_(self):
        out = []
        for gen in self._general():
            out.append(f"<div><i>{gen}</i></div>")
        out.append("<table>")
        out.extend(self._variables(fmt="html"))
        out.append("</table>")
        return "\n".join(out)

    def _general(self):
        if isinstance(self.x, TibbleGrouped):
            groups = ", ".join((str(name) for name in self.x.group_vars))
            group_title = (
                "Rowwise" if isinstance(self.x, TibbleRowwise) else "Groups"
            )
            return (
                f"Rows: {self.x.shape[0]}",
                f"Columns: {self.x.shape[1]}",
                f"{group_title}: {groups} "
                f"[{self.x._datar['grouped'].grouper.ngroups}]",
            )

        return (
            f"Rows: {self.x.shape[0]}",
            f"Columns: {self.x.shape[1]}",
        )

    def _calculate_output_widths(self):
        colname_width = max(len(str(colname)) for colname in self.x.columns)
        dtype_width = max(len(str(dtype)) for dtype in self.x.dtypes) + 2
        self.colwidths = (colname_width, dtype_width)

    def _variables(self, fmt="str"):
        for col in self.x:
            yield self._format_variable(
                col,
                self.x[col].dtype,
                self.x[col].obj.values
                if isinstance(self.x[col], SeriesGroupBy)
                else self.x[col].values,
                fmt=fmt,
            )

    def _format_variable(self, col, dtype, data, fmt="str"):
        if fmt == "str":
            return self._format_variable_str(col, dtype, data)

        return self._format_variable_html(col, dtype, data)

    def _format_data(self, data):
        """Format the data for the glimpse view

        Formatting 10 elements in a batch in case of a long dataframe.
        Since we don't need to format all the data, but only the first a few
        till the line (terminal width or provided width) overflows.
        """
        out = ""
        placeholder = "…"
        i = 0
        chunk_size = 10
        while not out.endswith(placeholder) and i < data.size:
            if out:
                out += ", "
            out += ", ".join(
                self.formatter(d) for d in data[i:i + chunk_size]
            )
            i += chunk_size
            out = textwrap.shorten(
                out,
                break_long_words=True,
                break_on_hyphens=True,
                width=self.width - 4 - sum(self.colwidths),
                placeholder=placeholder,
            )
        return out

    def _format_variable_str(self, col, dtype, data):
        name_col = col.ljust(self.colwidths[0])
        dtype_col = f'<{dtype}>'.ljust(self.colwidths[1])
        data_col = self._format_data(data)
        return f". {name_col} {dtype_col} {data_col}"

    def _format_variable_html(self, col, dtype, data):
        name_col = f". <b>{col}</b>"
        dtype_col = f"<i>&lt;{dtype}&gt;</i>"
        data_col = html.escape(self._format_data(data))
        return (
            f"<tr><th style=\"text-align: left\">{name_col}</th>"
            f"<td style=\"text-align: left\">{dtype_col}</td>"
            f"<td style=\"text-align: left\">{data_col}</td></tr>"
        )


@register_verb(DataFrame)
def glimpse(x, width=None, formatter=formatter):
    """Get a glimpse of your data

    Args:
        x: An object to glimpse at.
        width: Width of output, defaults to the width of the console.
        formatter: A single-dispatch function to format a single element.
    """
    return Glimpse(x, width=width, formatter=formatter)
