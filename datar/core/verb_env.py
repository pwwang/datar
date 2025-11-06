"""Utilities for handling verb registration with environment variable support"""
from __future__ import annotations

import os
from functools import wraps
from typing import Callable

from pipda import register_verb as _pipda_register_verb
from pipda.utils import TypeHolder


def _get_ast_fallback_from_env(func_name: str) -> str | None:
    """Get ast_fallback value from environment variables.
    
    Checks for per-verb environment variable first, then falls back to global.
    
    Args:
        func_name: The name of the function being registered
        
    Returns:
        The ast_fallback value from environment variables, or None if not set
    """
    # Convert function name to uppercase for environment variable
    # e.g., "select" -> "SELECT", "filter_" -> "FILTER"
    verb_name = func_name.rstrip("_").upper()
    
    # Check for per-verb environment variable first
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


def register_verb(
    cls=TypeHolder,
    *,
    func: Callable = None,
    context=None,
    kw_context=None,
    name: str = None,
    qualname: str = None,
    doc: str = None,
    module: str = None,
    dependent: bool = False,
    ast_fallback: str = None,
) -> Callable:
    """Register a verb with environment variable support for ast_fallback.
    
    This is a wrapper around pipda's register_verb that adds support for
    environment variables to control the ast_fallback behavior.
    
    Environment variables:
        DATAR_VERB_AST_FALLBACK: Global fallback for all verbs
        DATAR_<VERB>_AST_FALLBACK: Per-verb fallback (takes precedence)
        
    Valid values for ast_fallback:
        - "piping": Assume data >> verb(...) calling pattern
        - "normal": Assume verb(data, ...) calling pattern
        - "piping_warning": Assume piping, show warning (default)
        - "normal_warning": Assume normal, show warning
        - "raise": Raise an error when AST is not available
    
    Args:
        See pipda.register_verb for parameter documentation.
        
    Returns:
        The registered verb or a decorator to register a verb
    """
    # If func is provided directly (not used as decorator)
    if func is not None:
        env_fallback = _get_ast_fallback_from_env(func.__name__)
        if env_fallback and ast_fallback is None:
            ast_fallback = env_fallback
        
        return _pipda_register_verb(
            cls,
            func=func,
            context=context,
            kw_context=kw_context,
            name=name,
            qualname=qualname,
            doc=doc,
            module=module,
            dependent=dependent,
            ast_fallback=ast_fallback,
        )
    
    # When used as a decorator
    def decorator(f: Callable) -> Callable:
        env_fallback = _get_ast_fallback_from_env(f.__name__)
        if env_fallback and ast_fallback is None:
            final_ast_fallback = env_fallback
        else:
            final_ast_fallback = ast_fallback
        
        return _pipda_register_verb(
            cls,
            func=f,
            context=context,
            kw_context=kw_context,
            name=name,
            qualname=qualname,
            doc=doc,
            module=module,
            dependent=dependent,
            ast_fallback=final_ast_fallback,
        )
    
    return decorator
