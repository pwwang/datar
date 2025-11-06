"""Integration test to demonstrate the environment variable feature"""
import os
import pytest


def test_verb_ast_fallback_piping():
    """Test that DATAR_*_AST_FALLBACK works with piping mode"""
    from pipda import register_verb
    from datar.core.verb_env import get_verb_ast_fallback

    # Set environment variable for piping mode
    os.environ['DATAR_PLUS_AST_FALLBACK'] = 'piping'

    try:
        # Register a simple verb
        @register_verb(ast_fallback=get_verb_ast_fallback("plus"))
        def plus(x, y):
            return x + y

        # Test with exec to disable source code detection at runtime
        # In piping mode, piping call should work
        result = {}
        exec("result['val'] = 1 >> plus(1)", {"plus": plus, "result": result})
        assert result['val'] == 2

        # Normal call in piping mode returns a placeholder when AST is not available
        result = {}
        exec("result['val'] = plus(1, 1)", {"plus": plus, "result": result})
        # The result is a placeholder object, not the actual computation
        assert str(result['val']) == 'plus(., 1, 1)'

    finally:
        del os.environ['DATAR_PLUS_AST_FALLBACK']


def test_verb_ast_fallback_normal():
    """Test that DATAR_*_AST_FALLBACK works with normal mode"""
    from pipda import register_verb
    from datar.core.verb_env import get_verb_ast_fallback

    # Set environment variable for normal mode
    os.environ['DATAR_MINUS_AST_FALLBACK'] = 'normal'

    try:
        # Register a simple verb
        @register_verb(ast_fallback=get_verb_ast_fallback("minus"))
        def minus(x, y):
            return x - y

        # Test with exec to disable source code detection at runtime
        # In normal mode, normal call should work
        result = {}
        exec("result['val'] = minus(5, 3)", {"minus": minus, "result": result})
        assert result['val'] == 2

        # Piping call in normal mode raises TypeError when AST is not available
        result = {}
        with pytest.raises(TypeError):
            exec("result['val'] = 5 >> minus(3)", {"minus": minus, "result": result})

    finally:
        del os.environ['DATAR_MINUS_AST_FALLBACK']


def test_verb_ast_fallback_global():
    """Test that DATAR_VERB_AST_FALLBACK works as global fallback"""
    from pipda import register_verb
    from datar.core.verb_env import get_verb_ast_fallback

    # Set global environment variable
    os.environ['DATAR_VERB_AST_FALLBACK'] = 'piping'

    try:
        # Register verbs without specific env var
        @register_verb(ast_fallback=get_verb_ast_fallback("multiply"))
        def multiply(x, y):
            return x * y

        @register_verb(ast_fallback=get_verb_ast_fallback("divide"))
        def divide(x, y):
            return x / y

        # Both should use global piping mode
        result = {}
        exec("result['mul'] = 6 >> multiply(2)", {"multiply": multiply, "result": result})
        assert result['mul'] == 12

        exec("result['div'] = 10 >> divide(2)", {"divide": divide, "result": result})
        assert result['div'] == 5

    finally:
        del os.environ['DATAR_VERB_AST_FALLBACK']


def test_verb_ast_fallback_precedence():
    """Test that per-verb env var takes precedence over global"""
    from pipda import register_verb
    from datar.core.verb_env import get_verb_ast_fallback

    # Set global to piping and specific verb to normal
    os.environ['DATAR_VERB_AST_FALLBACK'] = 'piping'
    os.environ['DATAR_MODULO_AST_FALLBACK'] = 'normal'

    try:
        # Register verbs
        @register_verb(ast_fallback=get_verb_ast_fallback("power"))
        def power(x, y):
            return x ** y

        @register_verb(ast_fallback=get_verb_ast_fallback("modulo"))
        def modulo(x, y):
            return x % y

        # power should use global piping mode
        result = {}
        exec("result['pow'] = 2 >> power(3)", {"power": power, "result": result})
        assert result['pow'] == 8

        # modulo should use specific normal mode
        exec("result['mod'] = modulo(10, 3)", {"modulo": modulo, "result": result})
        assert result['mod'] == 1

    finally:
        del os.environ['DATAR_VERB_AST_FALLBACK']
        del os.environ['DATAR_MODULO_AST_FALLBACK']


if __name__ == "__main__":
    test_verb_ast_fallback_piping()
    test_verb_ast_fallback_normal()
    test_verb_ast_fallback_global()
    test_verb_ast_fallback_precedence()
    print("All integration tests passed!")
