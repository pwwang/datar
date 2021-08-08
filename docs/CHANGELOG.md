## 0.4.3

- Adopt `pipda` `0.4.5`
- Make dataset names case-insensitive;
- Add datasets: `ToothGrowth`, `economics`, `economics_long`, `faithful`, `faithfuld`, `luv_colours`, `midwest`, `mpg`, `msleep`, `presidential`, `seals`, and `txhousing`
- Change `datasets.all_datasets()` to `datasets.list_datasets()`
- Make sure `assume_all_piping` mode works internally: #45

## 0.4.2

- Adopt `pipda` 0.4.4
- Add `varname` to dependency to close #30
- Rename `datar.datar_versions` to `datar.get_versions`
- Port a set of functions from r-base, incluing:
    - `prod`, `sign`, `signif`, `trunc`, `exp`, `log`, `log2`, `log10`, `log1p`,
    - `is_finite`, `is_infinite`, `is_nan`,
    - `match`,
    - `startswith`, `endswith`, `strtoi`, `chartr`, `tolower`, `toupper`,
    - `max_col`


## 0.4.1

- Don't use piping syntax internally (`>>=`)
- Add `python`, `numpy` and `datar` version to `datar.datar_versions()`
- Fix #40: anti_join/semi_join not working when by is column mapping

## 0.4.0

- Adopt `pipda` v0.4.2

Performance improved:
- Refactor core.grouped to adopt pandas's groupby
- Try to use `DataFrame.agg()`/`DataFrameGroupBy.agg()` when function applied on a single columns (Related issues: #27, #33, #37)

Fixed:
- Fix when `data` or `context` as new column name for `mutate()`
- Fix SettingwithCopyWarning in pivot_longer
- Use regular calling internally to make sure it works in some cases that node cannot be detected (ie `Gooey`/`%%timeit` in jupyter)

Added:
- `datar.datar_versions()` to show versions of related packages for bug reporting.

## 0.3.2
- Adopt `pipda` v0.4.1 to fix `getattr()` failure for operater-connected expressions (#38)
- Add `str_dtype` argument to `as_character()` to partially fix #36
- Update license in `core._frame_format_patch` (#28)

## 0.3.1
- Adopt `pipda` v0.4.0
- Change argument `_dtypes` to `dtypes_` for tibble-families

## 0.3.0
- Adopt `pipda` v0.3.0

Breaking changes:

- Rename argument `dtypes` of `unchop` and `unnest` back to `ptype`
- Change all `_base0` to `base0_`
- Change argument `how` of `tidyr.drop_na` to `how_`

## 0.2.3
- Fix compatibility with `pandas` `v1.2.0~4` (#20, thanks to @antonio-yu)
- Fix base.table when inputs are factors and exclude is NA;
- Add base.scale/col_sums/row_sums/col_means/row_means/col_sds/row_sds/col_medians/row_medians

## 0.2.2
- Use a better strategy warning for builtin name overriding.
- Fix index of subdf not dropped for mutate on grouped data
- Fix `names_glue` not working with single `values_from` for `tidyr.pivot_wider`
- Fix `base.paste` not registered
- Fix `base.grep`/`grepl` on NA values
- Make `base.sub`/`gsub` return scalar when inputs are scalar strings

## 0.2.1
- Use observed values for non-observsed value match for group_data instead of NAs, which might change the dtype.
- Fix tibble recycling values too early

## 0.2.0
Added:
- Add `base.which`, `base.bessel`, `base.special`, `base.trig_hb` and `base.string` modules
- Add Support for duplicated keyword arguments for dplyr.mutate/summarise by using `_` as suffix
- Warn when import python builtin names directly; ; Remove modkit dependency

Fixed:
- Fixed errors when use`a_1` as names for `"check_unique"` name repairs
- Fixed #14: `f.a.mean()` not applied to grouped data

Changed:
- Don't allow `from datar.datasets import *`
- Remove `modkit` dependency
- Reset `NaN` to `NA`
- Rename `base.getOption` to `base.get_option`
- Rename `stats.setNames` to `stats.set_names`

## 0.1.1
- Adopt `pipda` 0.2.8
- Allow `f.col1[f.col2==max(f.col2)]` like expression
- Add `base.which/cov/var`
- Fix `base.max`
- Add `datasets.ChickWeight`
- Allow `dplyr.across` to have plain functions passed with default `EVAL` context.

## 0.1.0
Added:
- `pandas.NA` as `NaN`
- Dtypes display when printing a dataframe (string, html, notebook)
- `zibble` to construct dataframes with names specified together, and values together.

Fixed:
- `base.diag()` on dataframes
- Data recycling when length is different from original data
- `datar.itemgetter()` not public

Changed:
- Behavior of `group_by()` with `_drop=False`. Invisible values will not mix with visible values of other columns

## 0.0.7
- Add dplyr rows verbs
- Allow mixed numbering (with `c()` and `f[...]`) for tibble construction
- Allow slice (`f[a:b]`) to be expanded into sequence for `EVAL` context
- Finish tidyr porting.

## 0.0.6
- Add `options`, `get_option` and `options_context` to `datar.base` to allow set/get global options
- Add options: `dplyr.summarise.inform`
- Add `base0_` argument to all related APIs
- Add `nycflights13` datasets
- Support slice_head/slice_tail for grouped data

## 0.0.5
- Add option index.base.0;
- Refactor Collection families

## 0.0.4
- Finish port of tibble.

## 0.0.3
- Add stats.weighted_mean
- Allow function to prefer recycling input or output for summarise

## 0.0.2
- Port verbs and functions from tidyverse/dplyr and test them with original cases
