"""Integration test to demonstrate the environment variable feature"""
import os
import sys
import subprocess
import tempfile


def test_env_var_integration():
    """Integration test showing environment variables in action"""
    # Create a simple test script that uses datar
    test_script = """
import os
from datar.core.verb_env import register_verb

# Set environment variable before importing verbs
os.environ['DATAR_TEST_VERB_AST_FALLBACK'] = 'piping'

@register_verb()
def test_verb(data):
    return data

# Verify the verb is registered
assert callable(test_verb)
print("SUCCESS: Environment variable integration test passed")
"""

    # Write to a temporary file
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.py', delete=False
    ) as f:
        temp_path = f.name
        f.write(test_script)

    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, temp_path],
            capture_output=True,
            text=True
        )

        # Check the result
        assert result.returncode == 0
        assert "SUCCESS" in result.stdout
    finally:
        # Clean up the temporary file
        os.unlink(temp_path)


if __name__ == "__main__":
    test_env_var_integration()
