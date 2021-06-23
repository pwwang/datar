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
- Add `_base0` argument to all related APIs
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
