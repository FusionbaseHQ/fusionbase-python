"""Fusionbase AI agents module.

This module provides pre-built agents using Fusionbase tools.
"""

try:
    # Check for LangGraph dependency
    import langgraph
except ImportError:
    raise ImportError("LangGraph is required for the agents module. "
                      "Please install the required dependencies: "
                      "pip install fusionbase[ai]")

from .company_research import create_company_research_agent
from .tool_presets import ToolPresets

__all__ = ["create_company_research_agent", "ToolPresets"]
