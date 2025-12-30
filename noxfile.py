"""Nox configuration for multi-Python testing."""

import nox

# Default sessions to run
nox.options.sessions = ["tests"]

# Python versions to test against (matching CI workflow)
PYTHON_VERSIONS = ["3.9", "3.10", "3.11"]


def install_with_poetry(session):
    """Install dependencies using Poetry."""
    session.install("poetry")
    session.run("poetry", "install", "--no-interaction", external=False)


@nox.session(python=PYTHON_VERSIONS)
def tests(session):
    """Run the test suite."""
    install_with_poetry(session)

    # Run pytest
    session.run(
        "poetry", "run", "pytest",
        "--no-header", "--no-summary", "-xvs",
        "tests/",
        *session.posargs,
    )


@nox.session(python=PYTHON_VERSIONS)
def lint(session):
    """Run linting checks."""
    install_with_poetry(session)

    # Pylint
    session.run(
        "poetry", "run", "pylint",
        "fusionbase", "tests",
        "--fail-on=E", "--fail-under=7.5",
    )

    # isort check
    session.run(
        "poetry", "run", "isort",
        "--check", "--profile", "google",
        "fusionbase/", "tests/",
    )

    # yapf check
    session.run(
        "poetry", "run", "yapf",
        "--style", "google", "--diff", "--recursive",
        "fusionbase/", "tests/",
    )


@nox.session(python="3.11")
def tests_quick(session):
    """Run tests on Python 3.11 only (quick local check)."""
    install_with_poetry(session)
    session.run(
        "poetry", "run", "pytest",
        "--no-header", "--no-summary", "-xvs",
        "tests/",
        *session.posargs,
    )
