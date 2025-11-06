# Environment Variable Support for Verb AST Fallback

This document explains how to use environment variables to control the fallback behavior for verbs when AST (Abstract Syntax Tree) is not retrievable.

## Overview

When the AST of a function call is not available (e.g., in some interactive environments or when code is executed dynamically), datar verbs need to make assumptions about how they were called. You can now control this behavior using environment variables.

## Environment Variables

### Global Configuration

Set the global fallback behavior for all verbs:

```bash
export DATAR_VERB_AST_FALLBACK="piping"
```

This will assume all datar verbs are called with the piping pattern: `data >> verb(...)`

### Per-Verb Configuration

You can also configure individual verbs:

```bash
# Assume select is called like: data >> select(...)
export DATAR_SELECT_AST_FALLBACK="piping"

# Assume mutate is called like: mutate(data, ...)
export DATAR_MUTATE_AST_FALLBACK="normal"
```

Per-verb environment variables take precedence over the global setting.

## Valid Values

The following values are supported for ast_fallback:

- `"piping"`: Assume `data >> verb(...)` calling pattern
- `"normal"`: Assume `verb(data, ...)` calling pattern
- `"piping_warning"`: Assume piping call, but show a warning (default)
- `"normal_warning"`: Assume normal call, but show a warning
- `"raise"`: Raise an error when AST is not available

## Examples

### Example 1: Set Global Piping Mode

Add to your `.bashrc` or shell configuration:

```bash
export DATAR_VERB_AST_FALLBACK="piping"
```

Then in Python:

```python
from datar.all import *

# This will work without warnings when AST is not available
iris >> select(f.Species, f.Sepal_Length)
```

### Example 2: Mixed Configuration

```bash
# Most verbs use piping
export DATAR_VERB_AST_FALLBACK="piping"

# But filter uses normal calling convention
export DATAR_FILTER_AST_FALLBACK="normal"
```

Then in Python:

```python
from datar.all import *

# select uses piping (from global setting)
iris >> select(f.Species)

# filter uses normal calling (from per-verb setting)
filter(iris, f.Sepal_Length > 5)
```

### Example 3: Raise Errors for Debugging

```bash
export DATAR_VERB_AST_FALLBACK="raise"
```

This will help you identify cases where AST is not available, which can be useful for debugging.

## Notes

- Environment variables are checked when the verb is registered (at import time).
- If you change environment variables after importing datar modules, you'll need to restart your Python session for the changes to take effect.
- If you explicitly specify `ast_fallback` in the `@register_verb()` decorator, it takes precedence over environment variables.
- Verb names with trailing underscores (e.g., `filter_`) should use the environment variable without the underscore (e.g., `DATAR_FILTER_AST_FALLBACK`).
