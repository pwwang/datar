
`datar` uses `pandas`' `DataFrameGroupBy` internally.

The `datar.core.grouped.DataFrameGroupBy` and `datar.core.grouped.DataFrameRowwise` are actually subclasses of `pandas.DataFrame`.

There are a couple of reasons that we do it this way:

1. it is easier to write single dispatch functions, as DataFrameGroupBy is now a
   subclass of pandas' DataFrame
2. it is easier to display the frame. We can use all utilities for frame to
   display. By `core._frame_format_patch.py`, we are also able to show the
   grouping information
3. it is possible for future optimizations


Known Issues:
- Due to https://github.com/pandas-dev/pandas/issues/35202
        Currently `dropna` is fixed to `True` of `df.groupby(...)`
        So no NAs will be kept in group vars
- `_drop = FALSE` does not work when there are multiple group vars,
        of which, even there is only one categorical variable.
- Since group vars are required in `DataFrame.groupby()`, so virtual
        groupings are not supported (passing group data directly).
- Groupby on a column with tuples creates a multiindex
        https://github.com/pandas-dev/pandas/issues/21340
- Order of group data/groups does not follow the categories/levels of
        a category group variable.
