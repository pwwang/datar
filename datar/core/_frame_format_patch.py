# BSD 3-Clause License

# Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc.
# and PyData Development Team
# All rights reserved.

# Copyright (c) 2011-2021, Open source contributors.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""Monkey-patch data frame format to
1. add dtypes next to column names when printing
2. collapse data frames when they are elements of a parent data frame.
"""
# code grabbed from pandas v1.2.5
# works with v1.3.0.
from pandas import DataFrame
from pandas.io.formats import format as fmt, html, string as stringf
from pandas.io.formats.html import (
    HTMLFormatter,
    NotebookFormatter,
    Mapping,
    MultiIndex,
    get_level_lengths,
)
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
)
from pandas.io.formats.string import StringFormatter
from pandas.io.formats.printing import pprint_thing
from pandas.core.dtypes.common import is_scalar
from pandas.core.dtypes.missing import isna

from .options import add_option

# pylint: skip-file

# TODO: patch more formatters

# pandas 1.2.0 doesn't have this function
def _trim_zeros_single_float(str_float: str) -> str:  # pragma: no cover
    """
    Trims trailing zeros after a decimal point,
    leaving just one if necessary.
    """
    str_float = str_float.rstrip("0")
    if str_float.endswith("."):
        str_float += "0"

    return str_float


class DatarDataFrameFormatter(DataFrameFormatter):  # pragma: no cover
    """Custom formatter for DataFrame"""

    @property
    def grouping_info(self) -> str:
        """Get the string representation of grouping info"""
        from .grouped import DataFrameGroupBy, DataFrameRowwise

        if isinstance(self.frame, DataFrameRowwise):
            return f"\n[Rowwise: {', '.join(self.frame._group_vars)}]"

        if isinstance(self.frame, DataFrameGroupBy):
            ngroups = self.frame._group_data.shape[0]
            return (
                f"\n[Groups: {', '.join(self.frame._group_vars)} (n={ngroups})]"
            )

        return None

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

        return [f"<{dtype}>"] + format_array(
            frame.iloc[:, i]._values,
            formatter,
            float_format=self.float_format,
            na_rep=self.na_rep,
            space=self.col_space.get(frame.columns[i]),
            decimal=self.decimal,
            leading_space=self.index,
        )


class DatarGenericArrayFormatter(GenericArrayFormatter):  # pragma: no cover
    """Generic Array Formatter to show DataFrame element in a cell in a
    collpased representation
    """

    def _format_strings(self) -> List[str]:
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
            lib.map_infer(vals, is_float)
            # vals may have 2 or more dimensions
            & np.all(notna(vals), axis=tuple(range(1, len(vals.shape))))
        )
        leading_space = self.leading_space
        if leading_space is None:
            leading_space = is_float_type.any()

        fmt_values = []
        for i, v in enumerate(vals):
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


class DatarHTMLFormatter(HTMLFormatter):  # pragma: no cover
    """Fix nrows as we added one more row (dtype)"""

    def _write_regular_rows(
        self, fmt_values: Mapping[int, List[str]], indent: int
    ) -> None:
        is_truncated_horizontally = self.fmt.is_truncated_horizontally
        is_truncated_vertically = self.fmt.is_truncated_vertically

        nrows = len(self.fmt.tr_frame) + 1

        if self.fmt.index:
            fmtter = self.fmt._get_formatter("__index__")
            if fmtter is not None:
                index_values = self.fmt.tr_frame.index.map(fmtter)
            else:
                index_values = self.fmt.tr_frame.index.format()
        # dtype row
        index_values.insert(0, "")

        row: List[str] = []
        for i in range(nrows):

            if is_truncated_vertically and i == (self.fmt.tr_row_num):
                str_sep_row = ["..."] * len(row)
                tags = (
                    {
                        j: 'style="font-style: italic;" '
                        for j, _ in enumerate(str_sep_row)
                    }
                    if i == 0
                    else None
                )

                self.write_tr(
                    str_sep_row,
                    indent,
                    self.indent_delta,
                    tags=tags,
                    nindex_levels=self.row_levels,
                )

            row = []
            if self.fmt.index:
                row.append(index_values[i])
            # see gh-22579
            # Column misalignment also occurs for
            # a standard index when the columns index is named.
            # Add blank cell before data cells.
            elif self.show_col_idx_names:
                row.append("")
            row.extend(fmt_values[j][i] for j in range(self.ncols))

            if is_truncated_horizontally:
                dot_col_ix = self.fmt.tr_col_num + self.row_levels
                row.insert(dot_col_ix, "...")

            tags = (
                {j: 'style="font-style: italic;" ' for j, _ in enumerate(row)}
                if i == 0
                else None
            )

            self.write_tr(
                row,
                indent,
                self.indent_delta,
                tags=tags,
                nindex_levels=self.row_levels,
            )

    def _write_hierarchical_rows(
        self, fmt_values: Mapping[int, List[str]], indent: int
    ) -> None:
        template = 'rowspan="{span}" valign="top"'

        is_truncated_horizontally = self.fmt.is_truncated_horizontally
        is_truncated_vertically = self.fmt.is_truncated_vertically
        frame = self.fmt.tr_frame
        nrows = len(frame) + 1

        assert isinstance(frame.index, MultiIndex)
        idx_values = frame.index.format(
            sparsify=False, adjoin=False, names=False
        )
        # add dtype row
        len_idx_values = len(idx_values)
        idx_values = list(zip(*idx_values))
        idx_values.insert(0, ("",) * len_idx_values)

        if self.fmt.sparsify:
            # GH3547
            sentinel = lib.no_default
            levels = frame.index.format(
                sparsify=sentinel, adjoin=False, names=False
            )
            levels = [("",) + level for level in levels]
            level_lengths = get_level_lengths(levels, sentinel)
            inner_lvl = len(level_lengths) - 1
            if is_truncated_vertically:
                # Insert ... row and adjust idx_values and
                # level_lengths to take this into account.
                ins_row = self.fmt.tr_row_num
                inserted = False
                for lnum, records in enumerate(level_lengths):
                    rec_new = {}
                    for tag, span in list(records.items()):
                        if tag >= ins_row:
                            rec_new[tag + 1] = span
                        elif tag + span > ins_row:
                            rec_new[tag] = span + 1

                            # GH 14882 - Make sure insertion done once
                            if not inserted:
                                dot_row = list(idx_values[ins_row - 1])
                                dot_row[-1] = "..."
                                idx_values.insert(ins_row, tuple(dot_row))
                                inserted = True
                            else:
                                dot_row = list(idx_values[ins_row])
                                dot_row[inner_lvl - lnum] = "..."
                                idx_values[ins_row] = tuple(dot_row)
                        else:
                            rec_new[tag] = span
                        # If ins_row lies between tags, all cols idx cols
                        # receive ...
                        if tag + span == ins_row:
                            rec_new[ins_row] = 1
                            if lnum == 0:
                                idx_values.insert(
                                    ins_row, tuple(["..."] * len(level_lengths))
                                )

                            # GH 14882 - Place ... in correct level
                            elif inserted:
                                dot_row = list(idx_values[ins_row])
                                dot_row[inner_lvl - lnum] = "..."
                                idx_values[ins_row] = tuple(dot_row)
                    level_lengths[lnum] = rec_new

                level_lengths[inner_lvl][ins_row] = 1
                for ix_col in range(len(fmt_values)):
                    fmt_values[ix_col].insert(ins_row, "...")
                nrows += 1

            for i in range(nrows):
                row = []
                tags = {}

                sparse_offset = 0
                j = 0
                for records, v in zip(level_lengths, idx_values[i]):
                    if i in records:
                        if records[i] > 1:
                            tags[j] = (
                                template.format(span=records[i])
                                if i > 0
                                else 'style="font-style: italic;" '
                            )
                    else:
                        sparse_offset += 1
                        continue

                    j += 1
                    row.append(v)

                row.extend(fmt_values[j][i] for j in range(self.ncols))
                if i == 0:
                    tags.update(
                        {
                            j + k: 'style="font-style: italic;" '
                            for k in range(self.ncols)
                        }
                    )

                if is_truncated_horizontally:
                    row.insert(
                        self.row_levels - sparse_offset + self.fmt.tr_col_num,
                        "...",
                    )

                self.write_tr(
                    row,
                    indent,
                    self.indent_delta,
                    tags=tags,
                    nindex_levels=len(levels) - sparse_offset,
                )
        else:
            row = []
            for i in range(len(frame) + 1):
                if is_truncated_vertically and i == (self.fmt.tr_row_num):
                    str_sep_row = ["..."] * len(row)
                    tags = (
                        {
                            j: 'style="font-style: italic;" '
                            for j, _ in enumerate(str_sep_row)
                        }
                        if i == 0
                        else None
                    )

                    self.write_tr(
                        str_sep_row,
                        indent,
                        self.indent_delta,
                        tags=tags,
                        nindex_levels=self.row_levels,
                    )

                idx_values = list(
                    zip(
                        *frame.index.format(
                            sparsify=False, adjoin=False, names=False
                        )
                    )
                )
                idx_values.insert(0, ("",) * len_idx_values)
                row = []
                row.extend(idx_values[i])
                row.extend(fmt_values[j][i] for j in range(self.ncols))
                if is_truncated_horizontally:
                    row.insert(self.row_levels + self.fmt.tr_col_num, "...")

                tags = (
                    {
                        j: 'style="font-style: italic;" '
                        for j, _ in enumerate(row)
                    }
                    if i == 0
                    else None
                )

                self.write_tr(
                    row,
                    indent,
                    self.indent_delta,
                    tags=tags,
                    nindex_levels=frame.index.nlevels,
                )

    def render(self) -> List[str]:
        """Render the df"""
        from .grouped import DataFrameGroupBy, DataFrameRowwise

        self._write_table()

        if isinstance(self.frame, DataFrameRowwise):
            self.write(f"<p>Rowwise: {self.frame._group_vars}</p>")
        elif isinstance(self.frame, DataFrameGroupBy):
            ngroups = self.frame._group_data.shape[0]
            self.write(f"<p>Groups: {self.frame._group_vars} (n={ngroups})</p>")

        if self.should_show_dimensions:
            by = chr(215)  # ×
            self.write(
                f"<p>{len(self.frame)} rows {by} {len(self.frame.columns)} "
                "columns</p>"
            )

        return self.elements


class DatarNotebookFormatter(DatarHTMLFormatter, NotebookFormatter):
    """Notebook Formatter"""


class DatarStringFormatter(StringFormatter):  # pragma: no cover
    """String Formatter"""

    def to_string(self) -> str:
        """To string representation"""
        text = self._get_string_representation()
        grouping_info = getattr(self.fmt, "grouping_info")

        if grouping_info and self.fmt.should_show_dimensions:
            return "".join(
                [
                    # self.fmt.dimensions_info has two leading "\n"
                    text,
                    "\n",
                    grouping_info,
                    self.fmt.dimensions_info[1:],
                ]
            )
        if grouping_info:
            return "".join([text, "\n", grouping_info])
        if self.fmt.should_show_dimensions:
            return "".join([text, self.fmt.dimensions_info])

        return text


def _patch(option: bool) -> None:  # pragma: no cover
    """Patch pandas?"""
    if option:
        # monkey-patch the formatter
        fmt.DataFrameFormatter = DatarDataFrameFormatter
        fmt.GenericArrayFormatter = DatarGenericArrayFormatter
        html.HTMLFormatter = DatarHTMLFormatter
        html.NotebookFormatter = DatarNotebookFormatter
        stringf.StringFormatter = DatarStringFormatter
    else:
        fmt.DataFrameFormatter = DataFrameFormatter
        fmt.GenericArrayFormatter = GenericArrayFormatter
        html.HTMLFormatter = HTMLFormatter
        html.NotebookFormatter = NotebookFormatter
        stringf.StringFormatter = StringFormatter


add_option("frame_format_patch", True, _patch)
