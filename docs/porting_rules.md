## API lifecycle

- APIs with superseded lifecycle are not ported.

    For example, `mutate_at` from `dplyr` is superseded by `mutate` and `across`.

- APIs with experimental lifecycle are tried to be ported.

    For example, `group_map` and related functions

## Function/Argument naming

- `.` in function/argument names are replced with `_`

    For example, `is.integer` is ported as `is_integer`. Argument `.drop` in `group_by` is replaced with `_drop`.

- `datar` specific arguments are named with `_` suffix. For example, `base0_`.

- camelCase style named functions are ported with snake_case named functions.

    For example: `getOption` to `get_option`.

## Extra arguments

In order to keep some python language features, or extend the APIs a little, a few APIs may come with extra arguments. For example, `how_` for `drop_na` is added to allow drop rows of a data frame with `any` or `all` values of in that row.
