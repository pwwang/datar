"""Monkey-patch data frame format to
1. add dtypes next to column names when printing
2. collapse data frames when they are elements of a parent data frame.
"""
from pandas import DataFrame
from pandas.io.formats import format as fmt, html
from pandas.io.formats.html import (
    HTMLFormatter,
    NotebookFormatter,
    Mapping,
    MultiIndex,
    get_level_lengths
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
    _trim_zeros_single_float
)
from pandas.io.formats.printing import pprint_thing
from pandas.core.dtypes.common import is_scalar
from pandas.core.dtypes.missing import isna

from .options import add_option

# pylint: disable=c-extension-no-member
# pylint: disable=invalid-name
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=consider-using-enumerate
# pylint: disable=too-many-nested-blocks

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

class DatarHTMLFormatter(HTMLFormatter):
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
                tags = {
                    j: 'style="font-style: italic;" '
                    for j, _ in enumerate(str_sep_row)
                } if i == 0 else None

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

            tags = {
                j: 'style="font-style: italic;" '
                for j, _ in enumerate(row)
            } if i == 0 else None

            self.write_tr(
                row, indent, self.indent_delta, tags=tags,
                nindex_levels=self.row_levels
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
            sparsify=False,
            adjoin=False,
            names=False
        )
        # add dtype row
        len_idx_values = len(idx_values)
        idx_values = list(zip(*idx_values))
        idx_values.insert(0, ("", ) * len_idx_values)

        if self.fmt.sparsify:
            # GH3547
            sentinel = lib.no_default
            levels = frame.index.format(
                sparsify=sentinel,
                adjoin=False,
                names=False
            )
            levels = [("", ) + level for level in levels]
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
                    tags.update({
                        j + k: 'style="font-style: italic;" '
                        for k in range(self.ncols)
                    })

                if is_truncated_horizontally:
                    row.insert(
                        self.row_levels - sparse_offset + self.fmt.tr_col_num,
                        "..."
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
                    tags = {
                        j: 'style="font-style: italic;" '
                        for j, _ in enumerate(str_sep_row)
                    } if i == 0 else None

                    self.write_tr(
                        str_sep_row,
                        indent,
                        self.indent_delta,
                        tags=tags,
                        nindex_levels=self.row_levels,
                    )

                idx_values = list(
                    zip(*frame.index.format(
                        sparsify=False,
                        adjoin=False,
                        names=False
                    ))
                )
                idx_values.insert(0, ("", ) * len_idx_values)
                row = []
                row.extend(idx_values[i])
                row.extend(fmt_values[j][i] for j in range(self.ncols))
                if is_truncated_horizontally:
                    row.insert(self.row_levels + self.fmt.tr_col_num, "...")

                tags = {
                    j: 'style="font-style: italic;" '
                    for j, _ in enumerate(row)
                } if i == 0 else None

                self.write_tr(
                    row,
                    indent,
                    self.indent_delta,
                    tags=tags,
                    nindex_levels=frame.index.nlevels,
                )

class DatarNotebookFormatter(DatarHTMLFormatter, NotebookFormatter):
    """Notebook Formatter"""

def _patch(option: bool) -> None:
    """Patch pandas?"""
    if option:
        # monkey-patch the formatter
        fmt.DataFrameFormatter = DatarDataFrameFormatter
        fmt.GenericArrayFormatter = DatarGenericArrayFormatter
        html.HTMLFormatter = DatarHTMLFormatter
        html.NotebookFormatter = DatarNotebookFormatter
    else:
        fmt.DataFrameFormatter = DataFrameFormatter
        fmt.GenericArrayFormatter = GenericArrayFormatter
        html.HTMLFormatter = HTMLFormatter
        html.NotebookFormatter = NotebookFormatter

add_option('frame_format_patch', True, _patch)
