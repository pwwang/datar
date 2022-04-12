"""Provides handle_import_names_conflict"""
from .options import get_option

WARNED = set()


def handle_import_names_conflict(imports, conflict_names):
    """Handle the import names conflict.

    Args:
        imports: The `locals()` of the importing module.
        conflict_names: The names that conflict with builtin names.
            There are always underscore-suffixed names existing in imports
            For example, `sum_` for `sum`
            When `import_names_conflict` is `'underscore_suffixed'`, we are
            always using `sum_` for `sum`. Directly importing `sum` will raise
            `ImportError`
            When `import_names_conflict` is `'warn'`, a warning will be shown
            when names from `conflict_names` are imported
            When `import_names_conflict` is `'silent'`, do nothing when names
            from `conflict_names` are imported

    Returns:
        All names for the module, defining what will be imported when doing
        `from module import *`.
        Getattr function for the module, that is used to transform
        `sum` to `sum_`.
    """
    _import_names_conflict = get_option("import_names_conflict")
    if _import_names_conflict == "underscore_suffixed":
        return [name for name in imports if not name.startswith("_")], None

    import sys
    from executing import Source
    from .utils import logger

    def _getattr(name: str):
        # Using get_option("import_names_conflict") to get the value
        # instead of `import_names_conflict`
        # OPTIONS changed in lifetime
        opt_maybe_changed = get_option("import_names_conflict")
        if (
            name == "__path__"
            or name not in conflict_names
            or opt_maybe_changed == "underscore_suffixed"
        ):
            raise AttributeError

        # from ... import xxx
        if (
            name not in WARNED
            and opt_maybe_changed == "warn"
            and not Source.executing(sys._getframe(1)).node
        ):
            WARNED.add(name)
            logger.warning(
                'Builtin name "%s" has been masked by datar.',
                name,
            )

        return imports[f"{name}_"]

    _all = [name for name in imports if not name.startswith("_")]
    _all.extend(conflict_names)
    return _all, _getattr
