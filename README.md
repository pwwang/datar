# plyrda

Probably the closest port of [tidyr][1] + [dplyr][2] in python, using [pipda][3].

## Installtion

```shell
pip install -U plyrda
```

## Progress & docs

See: https://dplyr.tidyverse.org/reference/index.html and See: https://tidyr.tidyverse.org/reference/index.html

### dplyr - One table verbs
- [x] [`arrange()`](https://pwwang.github.io/dplyrda/dplyr/arrange): Arrange rows by column values
- [ ] [`count()`](https://pwwang.github.io/dplyrda/dplyr/count) [`tally()`](https://pwwang.github.io/dplyrda/dplyr/count) [`add_count()`](https://pwwang.github.io/dplyrda/dplyr/count) [`add_tally()`](https://pwwang.github.io/dplyrda/dplyr/count): Count observations by group
- [ ] [`distinct()`](https://pwwang.github.io/dplyrda/dplyr/distinct): Subset distinct/unique rows
- [ ] [`filter()`](https://pwwang.github.io/dplyrda/dplyr/filter): Subset rows using column values
- [x] [`mutate()`](https://pwwang.github.io/dplyrda/dplyr/mutate) [`transmute()`](https://pwwang.github.io/dplyrda/dplyr/mutate): Create, modify, and delete columns
- [ ] [`pull()`](https://pwwang.github.io/dplyrda/dplyr/pull): Extract a single column
- [x] [`relocate()`](https://pwwang.github.io/dplyrda/dplyr/relocate): Change column order
- [ ] [`rename()`](https://pwwang.github.io/dplyrda/dplyr/rename) [`rename_with()`](https://pwwang.github.io/dplyrda/dplyr/rename): Rename columns
- [x] [`select()`](https://pwwang.github.io/dplyrda/dplyr/select): Subset columns using their names and types
- [x] [`summarise()`](https://pwwang.github.io/dplyrda/dplyr/summarise) [`summarize()`](https://pwwang.github.io/dplyrda/dplyr/summarise): Summarise each group to fewer rows
- [ ] [`slice()`](https://pwwang.github.io/dplyrda/dplyr/slice) [`slice_head()`](https://pwwang.github.io/dplyrda/dplyr/slice) [`slice_tail()`](https://pwwang.github.io/dplyrda/dplyr/slice) [`slice_min()`](https://pwwang.github.io/dplyrda/dplyr/slice) [`slice_max()`](https://pwwang.github.io/dplyrda/dplyr/slice) [`slice_sample()`](https://pwwang.github.io/dplyrda/dplyr/slice): Subset rows using their positions

### dplyr - Two table verbs
- [ ] [`bind_rows()`](https://pwwang.github.io/dplyrda/dplyr/bind) [`bind_cols()`](https://pwwang.github.io/dplyrda/dplyr/bind): Efficiently bind multiple data frames by row and column
- [ ] [`reexports`](https://pwwang.github.io/dplyrda/dplyr/reexports): Objects exported from other packages
- [ ] [`inner_join()`](https://pwwang.github.io/dplyrda/dplyr/mutate-joins) [`left_join()`](https://pwwang.github.io/dplyrda/dplyr/mutate-joins) [`right_join()`](https://pwwang.github.io/dplyrda/dplyr/mutate-joins) [`full_join()`](https://pwwang.github.io/dplyrda/dplyr/mutate-joins): Mutating joins
- [ ] [`nest_join()`](https://pwwang.github.io/dplyrda/dplyr/nest_join): Nest join
- [ ] [`semi_join()`](https://pwwang.github.io/dplyrda/dplyr/filter-joins) [`anti_join()`](https://pwwang.github.io/dplyrda/dplyr/filter-joins): Filtering joins

### dplyr - Grouping
- [x] [`group_by()`](https://pwwang.github.io/dplyrda/dplyr/group_by) [`ungroup()`](https://pwwang.github.io/dplyrda/dplyr/group_by): Group by one or more variables
- [ ] [`group_cols()`](https://pwwang.github.io/dplyrda/dplyr/group_cols): Select grouping variables
- [ ] [`rowwise()`](https://pwwang.github.io/dplyrda/dplyr/rowwise): Group input by rows

### dplyr - Vector functions
- [ ] [`across()`](https://pwwang.github.io/dplyrda/dplyr/across) [`c_across()`](https://pwwang.github.io/dplyrda/dplyr/across): Apply a function (or a set of functions) to a set of columns
- [ ] [`between()`](https://pwwang.github.io/dplyrda/dplyr/between): Do values in a numeric vector fall in specified range?
- [ ] [`case_when()`](https://pwwang.github.io/dplyrda/dplyr/case_when): A general vectorised if
- [ ] [`coalesce()`](https://pwwang.github.io/dplyrda/dplyr/coalesce): Find first non-missing element
- [ ] [`cumall()`](https://pwwang.github.io/dplyrda/dplyr/cumall) [`cumany()`](https://pwwang.github.io/dplyrda/dplyr/cumall) [`cummean()`](https://pwwang.github.io/dplyrda/dplyr/cumall): Cumulativate versions of any, all, and mean
- [x] [`desc()`](https://pwwang.github.io/dplyrda/dplyr/desc): Descending order
- [ ] [`if_else()`](https://pwwang.github.io/dplyrda/dplyr/if_else): Vectorised if
- [ ] [`lag()`](https://pwwang.github.io/dplyrda/dplyr/lead-lag) [`lead()`](https://pwwang.github.io/dplyrda/dplyr/lead-lag): Compute lagged or leading values
- [ ] [`order_by()`](https://pwwang.github.io/dplyrda/dplyr/order_by): A helper function for ordering window function output
- [ ] [`n()`](https://pwwang.github.io/dplyrda/dplyr/context) [`cur_data()`](https://pwwang.github.io/dplyrda/dplyr/context) [`cur_data_all()`](https://pwwang.github.io/dplyrda/dplyr/context) [`cur_group()`](https://pwwang.github.io/dplyrda/dplyr/context) [`cur_group_id()`](https://pwwang.github.io/dplyrda/dplyr/context) [`cur_group_rows()`](https://pwwang.github.io/dplyrda/dplyr/context) [`cur_column()`](https://pwwang.github.io/dplyrda/dplyr/context): Context dependent expressions
- [ ] [`n_distinct()`](https://pwwang.github.io/dplyrda/dplyr/n_distinct): Efficiently count the number of unique values in a set of vector
- [ ] [`na_if()`](https://pwwang.github.io/dplyrda/dplyr/na_if): Convert values to NA
- [ ] [`near()`](https://pwwang.github.io/dplyrda/dplyr/near): Compare two numeric vectors
- [ ] [`nth()`](https://pwwang.github.io/dplyrda/dplyr/nth) [`first()`](https://pwwang.github.io/dplyrda/dplyr/nth) [`last()`](https://pwwang.github.io/dplyrda/dplyr/nth): Extract the first, last or nth value from a vector
- [ ] [`row_number()`](https://pwwang.github.io/dplyrda/dplyr/ranking) [`ntile()`](https://pwwang.github.io/dplyrda/dplyr/ranking) [`min_rank()`](https://pwwang.github.io/dplyrda/dplyr/ranking) [`dense_rank()`](https://pwwang.github.io/dplyrda/dplyr/ranking) [`percent_rank()`](https://pwwang.github.io/dplyrda/dplyr/ranking) [`cume_dist()`](https://pwwang.github.io/dplyrda/dplyr/ranking): Windowed rank functions.
- [ ] [`recode()`](https://pwwang.github.io/dplyrda/dplyr/recode) [`recode_factor()`](https://pwwang.github.io/dplyrda/dplyr/recode): Recode values

### dplyr - Data
- [x] [`band_members`](https://pwwang.github.io/dplyrda/dplyr/band_members) [`band_instruments`](https://pwwang.github.io/dplyrda/dplyr/band_members) [`band_instruments2`](https://pwwang.github.io/dplyrda/dplyr/band_members): Band membership
- [x] [`starwars`](https://pwwang.github.io/dplyrda/dplyr/starwars): Starwars characters
- [x] [`storms`](https://pwwang.github.io/dplyrda/dplyr/storms): Storm tracks data

### dplyr - Remote tables
- [ ] [`auto_copy()`](https://pwwang.github.io/dplyrda/dplyr/auto_copy): Copy tables to same source, if necessary
- [ ] [`compute()`](https://pwwang.github.io/dplyrda/dplyr/compute) [`collect()`](https://pwwang.github.io/dplyrda/dplyr/compute) [`collapse()`](https://pwwang.github.io/dplyrda/dplyr/compute): Force computation of a database query
- [ ] [`copy_to()`](https://pwwang.github.io/dplyrda/dplyr/copy_to): Copy a local data frame to a remote src
- [ ] [`ident()`](https://pwwang.github.io/dplyrda/dplyr/ident): Flag a character vector as SQL identifiers
- [ ] [`explain()`](https://pwwang.github.io/dplyrda/dplyr/explain) [`show_query()`](https://pwwang.github.io/dplyrda/dplyr/explain): Explain details of a tbl
- [ ] [`tbl()`](https://pwwang.github.io/dplyrda/dplyr/tbl) [`is.tbl()`](https://pwwang.github.io/dplyrda/dplyr/tbl): Create a table from a data source
- [ ] [`sql()`](https://pwwang.github.io/dplyrda/dplyr/sql): SQL escaping.

### dplyr - Experimental

Experimental functions are a testing ground for new approaches that we believe to be worthy of greater exposure. There is no guarantee that these functions will stay around in the future, so please reach out if you find them useful.
- [ ] [`group_map()`](https://pwwang.github.io/dplyrda/dplyr/group_map) [`group_modify()`](https://pwwang.github.io/dplyrda/dplyr/group_map) [`group_walk()`](https://pwwang.github.io/dplyrda/dplyr/group_map): Apply a function to each group
- [ ] [`group_trim()`](https://pwwang.github.io/dplyrda/dplyr/group_trim): Trim grouping structure
- [ ] [`group_split()`](https://pwwang.github.io/dplyrda/dplyr/group_split): Split data frame by groups
- [ ] [`with_groups()`](https://pwwang.github.io/dplyrda/dplyr/with_groups): Perform an operation with temporary groups

### dplyr - Questioning


We have our doubts about questioning functions. We’re not certain that they’re inadequate, or we don’t have a good replacement in mind, but these functions are at risk of removal in the future.
- [ ] [`all_equal()`](https://pwwang.github.io/dplyrda/dplyr/all_equal): Flexible equality comparison for data frames

### dplyr - Superseded

Superseded functions have been replaced by new approaches that we believe to be superior, but we don’t want to force you to change until you’re ready, so the existing functions will stay around for several years.
- [ ] [`sample_n()`](https://pwwang.github.io/dplyrda/dplyr/sample_n) [`sample_frac()`](https://pwwang.github.io/dplyrda/dplyr/sample_n): Sample n rows from a table
- [ ] [`top_n()`](https://pwwang.github.io/dplyrda/dplyr/top_n) [`top_frac()`](https://pwwang.github.io/dplyrda/dplyr/top_n): Select top (or bottom) n rows (by value)
- [ ] [`scoped`](https://pwwang.github.io/dplyrda/dplyr/scoped): Operate on a selection of variables
- [ ] [`arrange_all()`](https://pwwang.github.io/dplyrda/dplyr/arrange_all) [`arrange_at()`](https://pwwang.github.io/dplyrda/dplyr/arrange_all) [`arrange_if()`](https://pwwang.github.io/dplyrda/dplyr/arrange_all): Arrange rows by a selection of variables
- [ ] [`distinct_all()`](https://pwwang.github.io/dplyrda/dplyr/distinct_all) [`distinct_at()`](https://pwwang.github.io/dplyrda/dplyr/distinct_all) [`distinct_if()`](https://pwwang.github.io/dplyrda/dplyr/distinct_all): Select distinct rows by a selection of variables
- [ ] [`filter_all()`](https://pwwang.github.io/dplyrda/dplyr/filter_all) [`filter_if()`](https://pwwang.github.io/dplyrda/dplyr/filter_all) [`filter_at()`](https://pwwang.github.io/dplyrda/dplyr/filter_all): Filter within a selection of variables
- [ ] [`group_by_all()`](https://pwwang.github.io/dplyrda/dplyr/group_by_all) [`group_by_at()`](https://pwwang.github.io/dplyrda/dplyr/group_by_all) [`group_by_if()`](https://pwwang.github.io/dplyrda/dplyr/group_by_all): Group by a selection of variables
- [ ] [`mutate_all()`](https://pwwang.github.io/dplyrda/dplyr/mutate_all) [`mutate_if()`](https://pwwang.github.io/dplyrda/dplyr/mutate_all) [`mutate_at()`](https://pwwang.github.io/dplyrda/dplyr/mutate_all) [`transmute_all()`](https://pwwang.github.io/dplyrda/dplyr/mutate_all) [`transmute_if()`](https://pwwang.github.io/dplyrda/dplyr/mutate_all) [`transmute_at()`](https://pwwang.github.io/dplyrda/dplyr/mutate_all): Mutate multiple columns
- [ ] [`summarise_all()`](https://pwwang.github.io/dplyrda/dplyr/summarise_all) [`summarise_if()`](https://pwwang.github.io/dplyrda/dplyr/summarise_all) [`summarise_at()`](https://pwwang.github.io/dplyrda/dplyr/summarise_all) [`summarize_all()`](https://pwwang.github.io/dplyrda/dplyr/summarise_all) [`summarize_if()`](https://pwwang.github.io/dplyrda/dplyr/summarise_all) [`summarize_at()`](https://pwwang.github.io/dplyrda/dplyr/summarise_all): Summarise multiple columns
- [ ] [`all_vars()`](https://pwwang.github.io/dplyrda/dplyr/all_vars) [`any_vars()`](https://pwwang.github.io/dplyrda/dplyr/all_vars): Apply predicate to all variables
- [ ] [`vars()`](https://pwwang.github.io/dplyrda/dplyr/vars): Select variables


### tidyr - Pivoting


**Pivoting** changes the representation of a rectangular dataset, without changing the data inside of it.
- [x] [`pivot_longer()`](https://pwwang.github.io/dplyrda/tidyr/$1): Pivot data from wide to long
- [ ] [`pivot_wider()`](https://pwwang.github.io/dplyrda/tidyr/$1): Pivot data from long to wide
- [ ] [`spread()`](https://pwwang.github.io/dplyrda/tidyr/$1): Spread a key-value pair across multiple columns
- [ ] [`gather()`](https://pwwang.github.io/dplyrda/tidyr/$1): Gather columns into key-value pairs

### tidyr - Rectangling


**Rectangling** turns deeply nested lists into tidy tibbles.
- [ ] [`hoist()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unnest_longer()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unnest_wider()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unnest_auto()`](https://pwwang.github.io/dplyrda/tidyr/$1): Rectangle a nested list into a tidy tibble

### tidyr - Nesting


**Nesting** uses alternative representation of grouped data where a group becomes a single row containing a nested data frame.
- [ ] [`nest()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unnest()`](https://pwwang.github.io/dplyrda/tidyr/$1): Nest and unnest
- [ ] [`nest_legacy()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unnest_legacy()`](https://pwwang.github.io/dplyrda/tidyr/$1): Legacy versions of `nest()` and `unnest()`

### tidyr - Character vectors


Multiple variables are sometimes pasted together into a single column, and these tools help you separate back out into individual columns.
- [ ] [`extract()`](https://pwwang.github.io/dplyrda/tidyr/$1): Extract a character column into multiple columns using regular expression groups
- [ ] [`separate()`](https://pwwang.github.io/dplyrda/tidyr/$1): Separate a character column into multiple columns with a regular expression or numeric locations
- [ ] [`separate_rows()`](https://pwwang.github.io/dplyrda/tidyr/$1): Separate a collapsed column into multiple rows
- [ ] [`unite()`](https://pwwang.github.io/dplyrda/tidyr/$1): Unite multiple columns into one by pasting strings together

### tidyr - Missing values


Tools for converting between implicit (absent rows) and explicit (`NA`) missing values, and for handling explicit `NA`s.
- [ ] [`complete()`](https://pwwang.github.io/dplyrda/tidyr/$1): Complete a data frame with missing combinations of data
- [ ] [`drop_na()`](https://pwwang.github.io/dplyrda/tidyr/$1): Drop rows containing missing values
- [ ] [`expand()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`crossing()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`nesting()`](https://pwwang.github.io/dplyrda/tidyr/$1): Expand data frame to include all possible combinations of values
- [ ] [`expand_grid()`](https://pwwang.github.io/dplyrda/tidyr/$1): Create a tibble from all combinations of inputs
- [ ] [`fill()`](https://pwwang.github.io/dplyrda/tidyr/$1): Fill in missing values with previous or next value
- [ ] [`full_seq()`](https://pwwang.github.io/dplyrda/tidyr/$1): Create the full sequence of values in a vector
- [ ] [`replace_na()`](https://pwwang.github.io/dplyrda/tidyr/$1): Replace NAs with specified values

### tidyr - Miscellanea

- [ ] [`chop()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unchop()`](https://pwwang.github.io/dplyrda/tidyr/$1): Chop and unchop
- [ ] [`pack()`](https://pwwang.github.io/dplyrda/tidyr/$1) [`unpack()`](https://pwwang.github.io/dplyrda/tidyr/$1): Pack and unpack
- [ ] [`uncount()`](https://pwwang.github.io/dplyrda/tidyr/$1): "Uncount" a data frame

### tidyr - Data

- [x] [`billboard`](https://pwwang.github.io/dplyrda/tidyr/$1): Song rankings for billboard top 100 in the year 2000
- [x] [`construction`](https://pwwang.github.io/dplyrda/tidyr/$1): Completed construction in the US in 2018
- [x] [`fish_encounters`](https://pwwang.github.io/dplyrda/tidyr/$1): Fish encounters
- [x] [`relig_income`](https://pwwang.github.io/dplyrda/tidyr/$1): Pew religion and income survey
- [x] [`smiths`](https://pwwang.github.io/dplyrda/tidyr/$1): Some data about the Smith family
- [x] [`table1`](https://pwwang.github.io/dplyrda/tidyr/$1) [`table2`](https://pwwang.github.io/dplyrda/tidyr/$1) [`table3`](https://pwwang.github.io/dplyrda/tidyr/$1) [`table4a`](https://pwwang.github.io/dplyrda/tidyr/$1) [`table4b`](https://pwwang.github.io/dplyrda/tidyr/$1) [`table5`](https://pwwang.github.io/dplyrda/tidyr/$1): Example tabular representations
- [x] [`us_rent_income`](https://pwwang.github.io/dplyrda/tidyr/$1): US rent and income data
- [x] [`who`](https://pwwang.github.io/dplyrda/tidyr/$1) [`population`](https://pwwang.github.io/dplyrda/tidyr/$1): World Health Organization TB data
- [x] [`world_bank_pop`](https://pwwang.github.io/dplyrda/tidyr/$1): Population data from the world bank

[1]: https://tidyr.tidyverse.org/index.html
[2]: https://dplyr.tidyverse.org/index.html
[3]: https://github.com/pwwang/pipda
