
`datar` doesn't use `pandas`' `DataFrameGroupBy`/`SeriesGroupBy` classes. Instead, we have our own `DataFrameGroupBy` class, which is actually a subclass of `DataFrame`, with 3 extra properties: `_group_data`, `_group_vars` and `_group_drop`, carring the grouping data, grouping variables/columns and whether drop the non-observable values. This is very similar to `grouped_df` from `dplyr`.

The reasons that we implement this are:

1. Pandas DataFrameGroupBy cannot handle mutilpe categorical columns as
        groupby variables with non-obserable values
2. It is very hard to retrieve group indices and data when doing apply
3. NAs unmatched in grouping variables
