#!/usr/bin/env python
"""Using Tool Presets

This example shows how to use pre-configured tool sets (presets) for common
research scenarios:
- Minimal: Just the basics for fast execution
- Financial: All financial analysis tools
- News: News and current events focused
- Comprehensive: Everything available

Tool presets make it easy to get started without manually selecting tools.
"""

import asyncio
import os
import sys

# Check for required environment variables
if not os.environ.get("OPENAI_API_KEY"):
    print("Error: OPENAI_API_KEY environment variable not set.")
    sys.exit(1)

if not os.environ.get("FUSIONBASE_API_KEY"):
    print("Error: FUSIONBASE_API_KEY environment variable not set.")
    sys.exit(1)

from langchain_openai import ChatOpenAI

from fusionbase import Fusionbase
from fusionbase.ai.agents import create_company_research_agent
from fusionbase.ai.agents import ToolPresets


async def example_minimal_preset():
    """Use the minimal tool preset."""
    print("\n=== Minimal Tool Preset ===")
    print("Tools: organization_search, organization_detail, network, related_persons")

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=ToolPresets.minimal_researcher_tools(),
        max_iterations=3,
        verbose=False
    )

    query = "Find basic information about SAP SE"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"✅ Query: {query}")
    print(f"📄 Report length: {len(str(result))} characters")


async def example_financial_preset():
    """Use the financial-focused tool preset."""
    print("\n=== Financial Tool Preset ===")
    print("Tools: All financial relations (KPIs, P&L, balance sheet, annual statements)")

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=ToolPresets.financial_researcher_tools(),
        max_iterations=5,
        verbose=False
    )

    query = "Analyze the financial health of Volkswagen AG"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"✅ Query: {query}")
    print(f"📄 Report length: {len(str(result))} characters")


async def example_news_preset():
    """Use the news-focused tool preset."""
    print("\n=== News Tool Preset ===")
    print("Tools: news, publications, web search (if SERP key available)")

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=ToolPresets.news_researcher_tools(),
        serp_api_key=os.environ.get("SERP_API_KEY"),
        max_iterations=5,
        verbose=False
    )

    query = "Find recent news and announcements about Daimler AG"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"✅ Query: {query}")
    print(f"📄 Report length: {len(str(result))} characters")


async def example_custom_preset():
    """Create a custom tool configuration by modifying a preset."""
    print("\n=== Custom Tool Configuration ===")
    print("Starting with financial preset and adding news tools")

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Start with financial tools
    custom_tools = ToolPresets.financial_researcher_tools()

    # Add news tool
    from fusionbase.ai.tools import news
    if news not in custom_tools:
        custom_tools.insert(-2, news)  # Insert before output tools

    print(f"Total tools configured: {len(custom_tools)}")

    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=custom_tools,
        max_iterations=5,
        verbose=False
    )

    query = "Analyze Siemens AG financial performance and recent news"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"✅ Query: {query}")
    print(f"📄 Report length: {len(str(result))} characters")


async def main():
    """Run all preset examples."""
    print("Tool Presets Examples")
    print("====================")
    print("\nAvailable presets:")
    print("- ToolPresets.minimal_researcher_tools()")
    print("- ToolPresets.financial_researcher_tools()")
    print("- ToolPresets.news_researcher_tools()")
    print("- ToolPresets.comprehensive_researcher_tools()")
    print("- ToolPresets.web_only_researcher_tools()")

    # Run examples
    await example_minimal_preset()
    await example_financial_preset()
    await example_news_preset()
    await example_custom_preset()

    print("\n✅ All examples completed!")
    print("\nBenefits of tool presets:")
    print("- Quick setup for common use cases")
    print("- Consistent tool configurations")
    print("- Easy to extend or customize")
    print("- Reduces boilerplate code")


if __name__ == "__main__":
    asyncio.run(main())
