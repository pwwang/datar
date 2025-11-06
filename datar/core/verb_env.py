"""Utilities for getting verb AST fallback from environment variables"""
from __future__ import annotations

import os


def get_verb_ast_fallback(verb: str) -> str | None:
    """Get ast_fallback value from environment variables.

    Checks for per-verb environment variable first, then falls back to global.

    Args:
        verb: The name of the verb (e.g., "mutate", "select", "filter")

    Returns:
        The ast_fallback value from environment variables, or None if not set

    Example:
        >>> @register_verb(ast_fallback=get_verb_ast_fallback("mutate"))
        >>> def mutate(...):
        ...     pass
    """
    # Convert verb name to uppercase, removing trailing underscore if present
    # e.g., "select" -> "SELECT", "filter_" -> "FILTER"
    verb_name = verb.rstrip("_").upper()

    # Check for per-verb environment variable first
    # e.g., DATAR_MUTATE_AST_FALLBACK
    per_verb_key = f"DATAR_{verb_name}_AST_FALLBACK"
    per_verb_value = os.environ.get(per_verb_key)
    if per_verb_value:
        return per_verb_value

    # Fall back to global environment variable
    global_key = "DATAR_VERB_AST_FALLBACK"
    global_value = os.environ.get(global_key)
    if global_value:
        return global_value

    return None
