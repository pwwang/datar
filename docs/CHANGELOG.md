# Change Log

## 0.15.16

- refactor: migrate poetry to uv for dep management
- fix: correct `__all__` declaration in `__init__.py`

## 0.15.15

- docs: update outdated notebook for filter (#223)
- chore: update python-simpleconf version to ^0.9

## 0.15.14

- chore: update python-simpleconf version to ^0.8

## 0.15.13

- feat: add environment variables to control verb AST fallback behavior (#219)

## 0.15.12

- chore: update datar-pandas to version ^0.6
  - fix: fix fct_recode and fct_collapse with ordered factor (pwwang/datar#216)
  - fix: handle MultiIndex in _agg_result_compatible (pwwang/datar#214)
  - feat: suport pandas.Grouper() for group_by (pwwang/datar#215)
- chore: update datar-arrow to version ^0.2
  - chore: bump pyarrow to ^20

## 0.15.11

- feat: add generic pipe() function to datar for applying custom functions in piping workflows (#212)
- chore: update dependances
- test: fix missing reframe from tests

## 0.15.10

- ci: update deployment environment to use ubuntu-latest
- feat: add dplyr.reframe (#210)
- docs: fix reference map links

## 0.15.9

- ci: ensure Python version is set for setup step
- chore: update numpy dependency to allow any version

## 0.15.8

- chore(deps): update python-simpleconf to version 0.7

## 0.15.7

- chore(deps): drop support for python3.8
- ci: update Python version matrix to drop 3.8 and add 3.11, 3.12
- chore(docs): add mkdocs and related dependencies for documentation generation

## 0.15.6

- deps: bump simplug to 0.4, datar-numpy to 0.3.4 and datar-pandas to 0.5.5
- tests: adopt pytest v8
- ci: use latest actions

## 0.15.5

- deps: bump datar-numpy to 0.3.3

## 0.15.4

- docs: fix typo in README.md (#197)
- docs: change `filter` to `filter_` in README.md
- docs: fix typo in data.md
- deps: bump datar-pandas to 0.5.4 (support pandas 2.2+)

## 0.15.3

- ⬆️ Bump pipda to 0.13.1

## 0.15.2

- ⬆️ Bump datar-pandas to 0.5.2 to fix `pip install datar[pandas]` not having numpy backend installed.

## 0.15.1

- ⬆️ Bump datar-pandas to 0.5.1
    - Dismiss ast warning for if_else.
    - Make scipy and wcwidth optional deps
    - Set seed in tests
    - Dismiss warnings of fillna with method for pandas2.1

## 0.15.0

- ✨ Add data who2, household, cms_patient_experience, and
cms_patient_care
- ⬆️ Bump datar-pandas to 0.5 to support pandas2 (#186)

## 0.14.0

- ⬆️ Bump pipda to 0.13
- 🍱 Support dplyr up to 1.1.3
- 👽️ Align `rows_*()` verbs to align with dplyr 1.1.3 (#188)
- 🔧 Update pyproject.toml to generate setup.py for poetry

## 0.13.1

- 🎨 Allow `datar.all.filter` regardless of `allow_conflict_names` (#184)

## 0.13.0

- 👷 Add scripts for codesandbox
- 💥 Change the option for conflict names (#184)

    There is no more warning for conflict names (python reserved names). By default, those names are suffixed with `_` (ie `filter_` instead of `filter`). You can still use the original names by setting `allow_conflict_names` to `True` in `datar.options()`.

    ```python
    from datar import options
    options(allow_conflict_names=True)
    from datar.all import *
    filter  # <function datar.dplyr.filter_ at 0x7f62303c8940>
    ```

## 0.12.2

- ➕ Add pyarrow backend
- 🐛 Exclude coverage for multiline version in `get_versions()`
- ⬆️ Bump up `python-simpleconf` to 0.6 so datar can be installed in Windows (#180)

## 0.12.1

- ⬆️ Bump datar-numpy to ^0.2

## 0.12.0

- 📝 Added import f to plotnine on README.md (#177)
- ⬆️ Drop support for python3.7
- ⬆️ Bump pipda to 0.12
- 🍱 Update storms data to 2020 (tidyverse/dplyr#5899)

## 0.11.2

- 📝 Add pypi downloads badge to README
- 📝 Fix github workflow badges for README
- 🐛 Add return type annotation to fix #173
- ⬆️ Bump python-slugify to v8

## 0.11.1

- 🐛 Fix `get_versions()` not showing plugin versions
- 🐛 Fix plugins not loaded when loading datasets
- 🚸 Add github issue templates

## 0.11.0

- 📝 Add testimonials and backend badges in README.md
- 🐛 Load entrypoint plugins only when APIs are called (#162)
- 💥 Rename `other` module to `misc`

## 0.10.3

- ⬆️ Bump simplug to 0.2.2
- ✨ Add `apis.other.array_ufunc` to support numpy ufuncs
- 💥 Change hook `data_api` to `load_dataset`
- ✨ Allow backend for `c[]`
- ✨ Add `DatarOperator.with_backend()` to select backend for operators
- ✅ Add tests
- 📝 Update docs for backend supports

## 0.10.2

- 🚑 Fix false warning when importing from all

## 0.10.1

- Pump simplug to 0.2

## 0.10.0

- Detach backend support, so that more backends can be supported easier in the future
  - numpy backend: https://github.com/pwwang/datar-numpy
  - pandas backend: https://github.com/pwwang/datar-pandas
- Adopt pipda 0.10 so that functions can be pipeable (#148)
- Support pandas 1.5+ (#148), but v1.5.0 excluded (see pandas-dev/pandas#48645)

## 0.9.1

- Pump pipda to 0.8.0 (fixes #149)

## 0.9.0

### Fixes

- Fix `weighted_mean` not handling group variables with NaN values (#137)
- Fix `weighted_mean` on `NA` raising error instead of returning `NA` (#139)
- Fix pandas `.groupby()` used internally not inheriting `sort`, `dropna` and `observed` (#138, #142)
- Fix `mutate/summarise` not counting references inside function as used for `_keep` `"used"/"unused"`
- Fix metadata `_datar` of nested `TibbleGrouped` not frozen

### Breaking changes

- Refactor `core.factory.func_factory()` (#140)
- Use `base.c[...]` for range short cut, instead of `f[...]`
- Use `tibble.fibble()` when constructing `Tibble` inside a verb, instead of `tibble.tibble()`
- Make `n` a keyword-only argument for `base.ntile`

### Deprecation

- Deprecate `verb_factory`, use `register_verb` from `pipda` instead
- Deprecate `base.data_context`

### Dependences

- Adopt `pipda` `v0.7.1`
- Remove `varname` dependency
- Install `pdtypes` by default

## 0.8.6

- 🐛 Fix weighted_mean not working for grouped data (#133)
- ✅ Add tests for weighted_mean on grouped data
- ⚡️ Optimize distinct on existing columns (#128)

## 0.8.5

- 🐛 Fix columns missing after Join by same columns using mapping (#122)

## 0.8.4

- ➖ Add optional deps to extras so they aren't installed by default
- 🎨 Giva better message when optional packages not installed

## 0.8.3

- ⬆️ Upgrade pipda to v0.6
- ⬆️️ Upgrade thon-simpleconf to 5.5

## 0.8.2

- ♻️ Move `glimpse` to `dplyr` (as `glimpse` is a `tidyverse-dplyr` [API](https://dplyr.tidyverse.org/reference/glimpse.html))
- 🐛 Fix `glimpse()` output not rendering in qtconsole (#117)
- 🐛 Fix `base.match()` for pandas 1.3.0
- 🐛 Allow `base.match()` to work with grouping data (#115)
- 📌 Use `rtoml` (`python-simpleconf`) instead of `toml` (See https://github.com/pwwang/toml-bench)
- 📌 Update dependencies

## 0.8.1

- 🐛 Fix `month_abb` and `month_name` being truncated (#112)
- 🐛 Fix `unite()` not keeping other columns (#111)

## 0.8.0

- ✨ Support `base.glimpse()` (#107, machow/siuba#409)
- 🐛 Register `base.factor()` and accept grouped data (#108)
- ✨ Allow configuration file to save default options
- 💥 Replace option `warn_builtin_names` with `imiport_names_conflict` (#73)
- 🩹 Attach original `__module__` to `func_factory` registed functions
- ⬆️ Bump `pipda` to `0.5.9`

## 0.7.2

- ✨ Allow tidyr.unite() to unite multiple columns into a list, instead of join them (#105)
- 🩹 Typos in argument names of tidyr.pivot_longer() (#104)
- 🐛 Fix base.sprintf() not working with Series (#102)

## 0.7.1

- 🐛 Fix settingwithcopywarning in tidyr.pivot_wider()
- 📌 Pin deps for docs
- 💚 Don't upload coverage in PR
- 📝 Fix typos in docs (#99, #100) (Thanks to @pdwaggoner)


## 0.7.0

- ✨ Support `modin` as backend
- ✨ Add `_return` argument for `datar.options()`
- 🐛 Fix `tidyr.expand()` when `nesting(f.name)` as argument

## 0.6.4

### Breaking changes

- 🩹 Make `base.ntile()` labels 1-based (#92)

### Fixes

- 🐛 Fix `order_by` argument for `dplyr.lead-lag`

### Enhancements

- 🚑 Allow `base.paste/paste0()` to work with grouped data
- 🩹 Change dtypes of `base.letters/LETTERS/month_abb/month_name`

### Housekeeping

- 📝 Update and fix reference maps
- 📝 Add `environment.yml` for binder to work
- 📝 Update styles for docs
- 📝 Update styles for API doc in notebooks
- 📝 Update README for new description about the project and add examples from StackOverflow


## 0.6.3

- ✨ Allow `base.c()` to handle groupby data
- 🚑 Allow `base.diff()` to work with groupby data
- ✨ Allow `forcats.fct_inorder()` to work with groupby data
- ✨ Allow `base.rep()`'s arguments `length` and `each` to work with grouped data
- ✨ Allow `base.c()` to work with grouped data
- ✨ Allow `base.paste()`/`base.paste0()` to work with grouped data
- 🐛 Force `&/|` operators to return boolean data
- 🚑 Fix `base.diff()` not keep empty groups
- 🐛 Fix recycling non-ordered grouped data
- 🩹 Fix `dplyr.count()/tally()`'s warning about the new name
- 🚑 Make `dplyr.n()` return groupoed data
- 🐛 Make `dplyr.slice()` work better with rows/indices from grouped data
- 🩹 Make `dplyr.ntile()` labels 1-based
- ✨ Add `datar.attrgetter()`, `datar.pd_str()`, `datar.pd_cat()` and `datar.pd_dt()`

## 0.6.2

- 🚑 Fix #87 boolean operator losing index
- 🚑 Fix false alarm from `rename()`/`relocate()` for missing grouping variables (#89)
- ✨ Add `base.diff()`
- 📝 [doc] Update/Fix doc for case_when (#87)
- 📝 [doc] Fix links in reference map
- 📝 [doc] Update docs for `dplyr.base`

## 0.6.1

- 🐛 Fix `rep(df, n)` producing a nested df
- 🐛 Fix `TibbleGrouped.__getitem__()` not keeping grouping structures

## 0.6.0

### General
- Adopt `pipda` 0.5.7
- Reimplement the split-apply-combine rule to solve all performance issues
- Drop support for pandas v1.2, require pandas v1.3+
- Remove all `base0_` options and all indices are now 0-based, except `base.seq()`, ranks and their variants
- Remove messy type annotations for now, will add them back in the future
- Move implementation of data type display for frames in terminal and notebook to `pdtypes` package
- Change all arguments end with "_" to arguments start with it to avoid confusion
- Move module `datar.stats` to `datar.base.stats`
- Default all `na_rm` arguments to `True`
- Rename all `ptype` arguments for `tidyr` verbs into `dtypes`

### Details
- Introduct new API to register function `datar.core.factory.func_factory()`
- Aliase `register_verb` and `register_func` as `verb_factory` and `context_func_factory` in `datar.core.factory`
- Expose `options`, `options_context`, `add_option` and `get_option` in `datar/__init__.py` and remove them from `datar.base`
- Attach `pipda.options` to `datar.options`
- Move `head` and `tail` from `datar.utils` to `datar.base`
- Remove redundant `unique` implentation from `datar.base.seq`
- Add `datar.core.factory.func_factory()` for developers to register function that works with different types of data (`NDFrame`, `GropuBy`, etc)
- Not ensure NAs after NA for `base.cumxxx()` families any more
- Remove `set_names` from `datar.stats`, use `names(df, <new names>)` from `datar.base` instead
- Optimize `intersect`, `union`, `setdiff`, `append` from `datar.base`
- Keep grouping variables for `intersect`, `union`, `setdiff` and `union_all` when `y` is a grouped df, even when `x` is not
- Remove `drop_index` from `datar.datar`, use `datar.tibble.remove_rownames/remove_index/drop_index` instead
- Add `assert_tibble_equal()` in `datar.testing` to test whether 2 tibbles are equal
- `rep()` now works with frames
- `c_across()` now returns a rowwise df to work with functions that apply to df on `axis=1`
- `datar.dplyr.order_by()` now only works like it does in `r-dplyr` and only in side a verb
- `datar.dplyr.group_by()` detauls `_sort` to `False` for speed
- Only raise error for duplicated column names when selected by column name instead of index
- `base.scale()` returns a series rather than a frame when works with a series
- Other fixes and optimizations

## 0.5.6

- 🐛 Hotfix for types registered for base.proportions (#77)
- 👽️ Fix for pandas 1.4

## 0.5.5

- Fix #71: semi_join returns duplicated rows

## 0.5.4

- Fix `filter()` restructures group_data incorrectly (#69)

## 0.5.3

- ⚡️ Optimize dplyr.arrange when data are series from the df itself
- 🐛 Fix sub-df order of apply for grouped df (#63)
- 📝 Update doc for argument by for join functions (#62)
- 🐛 Fix mean() with option na_rm=False does not work (#65)

## 0.5.2
More of a maintenance release.

- 🔧 Add metadata for datasets
- 🔊 Send logs to stderr, instead of stdout
- 📌Pin dependency versions
- 🚨 Switch linter to flake8
- 📝 Update some docs to fit `datar-cli`

## 0.5.1
- Add documentation about "blind" environment (#45, #54, #55)
- Change `base.as_date()` to return pandas datetime types instead python datetime types (#56)
- Add `base.as_pd_date()` to be an alias of `pandas.to_datetime()` (#56)
- Expose `trimws` to `datar.all` (#58)

## 0.5.0

Added:

- Added `forcats` (#51 )
- Added `base.is_ordered()`, `base.nlevels()`, `base.ordered()`, `base.rank()`, `base.order()`, `base.sort()`, `base.tabulate()`, `base.append()`, `base.prop_table()` and `base.proportions()`
- Added `gss_cat` dataset

Fixed:

- Fixed an issue when `Collection` dealing with `numpy.int_`

Enhanced:

- Added `base0_` argument for `datar.get()`
- Passed `__calling_env` to registered functions/verbs when used internally (this makes sure the library to be robust in different environments)


## 0.4.4

- Adopt `varname` `v0.8.0`
- Add `base.make_names()` and `base.make_unique()`

## 0.4.3

- Adopt `pipda` `0.4.5`
- Make dataset names case-insensitive;
- Add datasets: `ToothGrowth`, `economics`, `economics_long`, `faithful`, `faithfuld`, `luv_colours`, `midwest`, `mpg`, `msleep`, `presidential`, `seals`, and `txhousing`
- Add `base.complete_cases()`
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
