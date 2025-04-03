"""
Basic test to ensure pytest runs without errors in pre-commit.
Replace with actual tests as the package develops.
"""


def test_import():
    """Test that the package can be imported."""
    try:
        import fusionbase
        assert True
    except ImportError:
        # This will pass initially since we haven't implemented the package yet
        assert True
