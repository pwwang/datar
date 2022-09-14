Options are used to change some behaviors in `datar`.

## Available options

### dplyr_summarise_inform

Default: `True`

With `dplyr.summarise()`, when `_groups` is not specified, a message is printed to inform the choice (`drop_last` or `keep`), based on the number of rows in the results.

See [https://dplyr.tidyverse.org/reference/summarise.html](https://dplyr.tidyverse.org/reference/summarise.html)

### import_names_conflict

What to do when there are conflicts importing names
- `warn` (default): show warnings
- `silent`: ignore the conflicts
- `underscore_suffixed`: add suffix `_` to the conflicting names
  (and don't do any warnings)

See also [Import datar/Warn abbout python reserved names](../import/#warn-about-python-reserved-names-to-be-overriden-by-datar)

### enable_pdtypes

Default: `True`

Whether to enable `pdtypes`, a package that shows data types right beneith the column names when a data frame is present in string, HTML or a jupyter notebook. See:

[https://github.com/pwwang/pdtypes](https://github.com/pwwang/pdtypes)

### backend

The backend for datar. `pandas` (default) or `modin`


## Configuration files

You can change the default behavior of datar by configuring a `.toml.toml` file in your home directory. For example, to always use underscore-suffixed names for conflicting names, you can add the following to your `~/.datar.toml` file:

```toml
import_names_conflict = "underscore_suffixed"
```

You can also have a project/directory-based configuration file (`./.datar.toml`) in your current working directory, which has higher priority than the home directory configuration file.
