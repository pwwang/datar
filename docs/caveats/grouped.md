Grouped frames are instances of `TibbleGrouped`, which is a subclass of `DataFrame`. It carries the `DataFrameGroupBy` object as meta data.
Unlike a normal `DataFrame` object that `df.a` gets a `Series` object for us, `gf`, an instance of `TibbleGrouped` returns a `SeriesGroupBy` object.

Rowwise frames are instances of `TibbleRowwise`, a subclass of `TibbleGrouped`.
A rowwise frame is grouped by a range of number of rows, so that each row appears in just one group. It also saves grouping variables in meta data so when a rowwise frame gets summarised, those grouping variables can be kept (like grouped frames.)


There are a couple of reasons that we do it this way:

1. it is easier to write single dispatch functions, as `TibbleGrouped` is now a
   subclass of pandas' DataFrame
2. it is easier to display the frame. We can use all utilities for frame to
   display.


Known Issues:
- Due to https://github.com/pandas-dev/pandas/issues/35202
        Currently `dropna` is fixed to `True` of `df.groupby(...)`
        So no NAs will be kept in group vars
- `_drop = False` does not work when there are multiple group vars,
        of which, even there is only one categorical variable.
- Groupby on a column with tuples creates a multiindex
        https://github.com/pandas-dev/pandas/issues/21340
- Order of group data/groups does not follow the categories/levels of
        a category group variable.
