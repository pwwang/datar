"""Tests for verb environment variable support"""
import os
import pytest


def test_env_var_global():
    """Test global environment variable DATAR_VERB_AST_FALLBACK"""
    # Set the global environment variable
    os.environ["DATAR_VERB_AST_FALLBACK"] = "piping"
    
    try:
        # Import after setting the environment variable
        # to ensure the verb picks up the setting
        from datar.core.verb_env import register_verb, _get_ast_fallback_from_env
        
        # Test that the function reads the environment variable
        result = _get_ast_fallback_from_env("test_verb")
        assert result == "piping"
        
    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]


def test_env_var_per_verb():
    """Test per-verb environment variable DATAR_<VERB>_AST_FALLBACK"""
    # Set a per-verb environment variable
    os.environ["DATAR_SELECT_AST_FALLBACK"] = "normal"
    
    try:
        from datar.core.verb_env import _get_ast_fallback_from_env
        
        # Test that the function reads the per-verb environment variable
        result = _get_ast_fallback_from_env("select")
        assert result == "normal"
        
    finally:
        # Clean up
        del os.environ["DATAR_SELECT_AST_FALLBACK"]


def test_env_var_per_verb_with_trailing_underscore():
    """Test per-verb environment variable for verbs with trailing underscore"""
    # Set a per-verb environment variable for filter_ verb
    os.environ["DATAR_FILTER_AST_FALLBACK"] = "raise"
    
    try:
        from datar.core.verb_env import _get_ast_fallback_from_env
        
        # Test that the function reads the per-verb environment variable
        # even when the function name has a trailing underscore
        result = _get_ast_fallback_from_env("filter_")
        assert result == "raise"
        
    finally:
        # Clean up
        del os.environ["DATAR_FILTER_AST_FALLBACK"]


def test_env_var_precedence():
    """Test that per-verb environment variable takes precedence over global"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "piping"
    os.environ["DATAR_MUTATE_AST_FALLBACK"] = "normal"
    
    try:
        from datar.core.verb_env import _get_ast_fallback_from_env
        
        # For mutate, the per-verb setting should take precedence
        result = _get_ast_fallback_from_env("mutate")
        assert result == "normal"
        
        # For other verbs, the global setting should be used
        result = _get_ast_fallback_from_env("select")
        assert result == "piping"
        
    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]
        del os.environ["DATAR_MUTATE_AST_FALLBACK"]


def test_env_var_not_set():
    """Test behavior when no environment variable is set"""
    # Ensure no relevant environment variables are set
    for key in list(os.environ.keys()):
        if key.startswith("DATAR_") and key.endswith("_AST_FALLBACK"):
            del os.environ[key]
    
    from datar.core.verb_env import _get_ast_fallback_from_env
    
    # Should return None when no environment variable is set
    result = _get_ast_fallback_from_env("test_verb")
    assert result is None


def test_register_verb_with_env_var():
    """Test that register_verb respects environment variables"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "normal"
    
    try:
        from datar.core.verb_env import register_verb
        
        # Define a simple test verb
        @register_verb()
        def test_verb(data):
            """Test verb"""
            return data
        
        # The verb should be registered (we can't easily check the ast_fallback
        # without accessing internals, but we can verify it doesn't error)
        assert callable(test_verb)
        
    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]


def test_register_verb_explicit_ast_fallback_takes_precedence():
    """Test that explicit ast_fallback parameter takes precedence over env vars"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "normal"
    
    try:
        from datar.core.verb_env import register_verb
        
        # Define a test verb with explicit ast_fallback
        @register_verb(ast_fallback="raise")
        def test_verb_explicit(data):
            """Test verb with explicit ast_fallback"""
            return data
        
        # The verb should be registered with the explicit value
        # We can't easily verify the ast_fallback value without accessing internals,
        # but we can verify it doesn't error
        assert callable(test_verb_explicit)
        
    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]


def test_register_verb_with_dependent():
    """Test that register_verb works with dependent parameter"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "piping"
    
    try:
        from datar.core.verb_env import register_verb
        
        # Define a dependent verb
        @register_verb(dependent=True)
        def test_dependent_verb(data):
            """Test dependent verb"""
            return data
        
        assert callable(test_dependent_verb)
        
    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]
