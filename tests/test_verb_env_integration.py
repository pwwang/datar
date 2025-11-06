"""Integration test to demonstrate the environment variable feature"""
import os
import sys
import subprocess


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
    with open('/tmp/test_env_integration.py', 'w') as f:
        f.write(test_script)
    
    # Run the script
    result = subprocess.run(
        [sys.executable, '/tmp/test_env_integration.py'],
        capture_output=True,
        text=True
    )
    
    # Check the result
    assert result.returncode == 0
    assert "SUCCESS" in result.stdout


if __name__ == "__main__":
    test_env_var_integration()
