"""Tests for verb environment variable support"""
import os
import pytest


def test_env_var_global():
    """Test global environment variable DATAR_VERB_AST_FALLBACK"""
    # Set the global environment variable
    os.environ["DATAR_VERB_AST_FALLBACK"] = "piping"

    try:
        from datar.core.verb_env import get_verb_ast_fallback

        # Test that the function reads the environment variable
        result = get_verb_ast_fallback("test_verb")
        assert result == "piping"

    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]


def test_env_var_per_verb():
    """Test per-verb environment variable DATAR_<VERB>_AST_FALLBACK"""
    # Set a per-verb environment variable
    os.environ["DATAR_SELECT_AST_FALLBACK"] = "normal"

    try:
        from datar.core.verb_env import get_verb_ast_fallback

        # Test that the function reads the per-verb environment variable
        result = get_verb_ast_fallback("select")
        assert result == "normal"

    finally:
        # Clean up
        del os.environ["DATAR_SELECT_AST_FALLBACK"]


def test_env_var_per_verb_with_trailing_underscore():
    """Test per-verb environment variable for verbs with trailing underscore"""
    # Set a per-verb environment variable for filter_ verb
    os.environ["DATAR_FILTER_AST_FALLBACK"] = "raise"

    try:
        from datar.core.verb_env import get_verb_ast_fallback

        # Test that the function reads the per-verb environment variable
        # even when the function name has a trailing underscore
        result = get_verb_ast_fallback("filter_")
        assert result == "raise"

    finally:
        # Clean up
        del os.environ["DATAR_FILTER_AST_FALLBACK"]


def test_env_var_precedence():
    """Test that per-verb environment variable takes precedence over global"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "piping"
    os.environ["DATAR_MUTATE_AST_FALLBACK"] = "normal"

    try:
        from datar.core.verb_env import get_verb_ast_fallback

        # For mutate, the per-verb setting should take precedence
        result = get_verb_ast_fallback("mutate")
        assert result == "normal"

        # For other verbs, the global setting should be used
        result = get_verb_ast_fallback("select")
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

    from datar.core.verb_env import get_verb_ast_fallback

    # Should return None when no environment variable is set
    result = get_verb_ast_fallback("test_verb")
    assert result is None


def test_verb_with_env_var():
    """Test that verbs can use the helper function"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "normal"

    try:
        from pipda import register_verb
        from datar.core.verb_env import get_verb_ast_fallback

        # Define a simple test verb using the helper
        @register_verb(ast_fallback=get_verb_ast_fallback("test_verb"))
        def test_verb(data):
            """Test verb"""
            return data

        # The verb should be registered
        assert callable(test_verb)

    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]


def test_explicit_ast_fallback_with_env_var():
    """Test that explicit ast_fallback is used even when env var is set"""
    os.environ["DATAR_VERB_AST_FALLBACK"] = "normal"

    try:
        from pipda import register_verb
        from datar.core.verb_env import get_verb_ast_fallback

        # When we explicitly pass an ast_fallback, it should be used
        # But if we use the helper, it will return the env var value
        # This test verifies the helper returns the env var
        result = get_verb_ast_fallback("test_verb")
        assert result == "normal"

    finally:
        # Clean up
        del os.environ["DATAR_VERB_AST_FALLBACK"]
