Options are used to change some behaviors in `datar`.

## Available options

### import_names_conflict

What to do when there are conflicts importing names

- `warn` (default): show warnings
- `silent`: ignore the conflicts
- `underscore_suffixed`: add suffix `_` to the conflicting names
  (and don't do any warnings)

See also [Import datar/Warn abbout python reserved names](../import/#warn-about-python-reserved-names-to-be-overriden-by-datar)

### backends

If you have multiple backends installed, you can use this option to specify which backends to use.

## Configuration files

You can change the default behavior of datar by configuring a `.toml.toml` file in your home directory. For example, to always use underscore-suffixed names for conflicting names, you can add the following to your `~/.datar.toml` file:

```toml
import_names_conflict = "underscore_suffixed"
```

You can also have a project/directory-based configuration file (`./.datar.toml`) in your current working directory, which has higher priority than the home directory configuration file.
