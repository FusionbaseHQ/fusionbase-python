"""Fusionbase AI - Intelligent tools for working with Fusionbase data.

This module provides tools and utilities for integrating Fusionbase
with AI systems like LangChain and agent-based architectures.
"""

# Check for required dependencies
try:
    import html2text  # type: ignore
    import langchain  # type: ignore
    import langchain_core  # type: ignore
except ImportError as e:
    if "html2text" in str(e):
        raise ImportError(
            "To use Fusionbase AI web content tools, please install the html2text package: "
            "pip install fusionbase[ai]"
        ) from e
    else:
        raise ImportError(
            "To use Fusionbase AI tools, please install the required dependencies: "
            "pip install fusionbase[ai]"
        )

# Import the tools module directly
from .tools import async_google_search
from .tools import async_organization_detail
from .tools import async_organization_search
from .tools import async_relation_detail
from .tools import async_relation_resolve
from .tools import async_relation_search
from .tools import async_web_content
from .tools import google_search
from .tools import organization_detail
from .tools import organization_search
from .tools import relation_detail
from .tools import relation_resolve
from .tools import relation_search
from .tools import web_content

__all__ = [
    "organization_search",
    "organization_detail",
    "async_organization_search",
    "async_organization_detail",
    "relation_search",
    "relation_detail",
    "relation_resolve",
    "async_relation_search",
    "async_relation_detail",
    "async_relation_resolve",
    "google_search",
    "async_google_search",
    "web_content",
    "async_web_content",
]
