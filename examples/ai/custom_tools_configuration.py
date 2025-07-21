#!/usr/bin/env python
"""Custom Tools Configuration Example

This example demonstrates how to configure which tools the Company Research Agent
has access to. You can create agents with:
- Minimal tools for faster execution
- Financial-focused tools for financial analysis
- Web-enhanced tools for comprehensive research

Learn how to optimize your agent for specific use cases.
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
from fusionbase.ai.tools import financial_kpi  # Core tools; Relation tools; Web tools (if you have SERP_API_KEY)
from fusionbase.ai.tools import google_search
from fusionbase.ai.tools import network
from fusionbase.ai.tools import news
from fusionbase.ai.tools import organization_detail
from fusionbase.ai.tools import organization_search
from fusionbase.ai.tools import related_persons
from fusionbase.ai.tools import web_content

# Note: Internal tools like GlobalFinding and ResearchReflection
# are now automatically included by the agent


async def example_minimal_agent():
    """Create an agent with minimal tools - only Fusionbase data, no web search."""
    print("\n=== Example 1: Minimal Agent (Fusionbase only) ===")

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Define minimal researcher tools - only Fusionbase tools
    minimal_researcher_tools = [
        # Core organization tools
        organization_search,
        organization_detail,

        # Selected relation tools
        financial_kpi,
        network,
        related_persons,

        # Note: Internal tools are automatically included
    ]

    # Create agent with custom tools
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=minimal_researcher_tools,  # Custom researcher tools
        max_iterations=3,
        verbose=True
    )

    # Test with a query
    query = "Find the financial KPIs and network information for BMW AG"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"\nResult preview: {str(result)[:200]}...")


async def example_financial_focused_agent():
    """Create an agent focused on financial data."""
    print("\n=== Example 2: Financial-Focused Agent ===")

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Import additional financial tools
    from fusionbase.ai.tools import annual_financial_statements
    from fusionbase.ai.tools import balance_sheet_accounts
    from fusionbase.ai.tools import profit_and_loss_account

    # Define financial-focused researcher tools
    financial_researcher_tools = [
        # Basic search
        organization_search,
        organization_detail,

        # All financial relation tools
        financial_kpi,
        profit_and_loss_account,
        balance_sheet_accounts,
        annual_financial_statements,
    ]

    # Create agent with financial focus
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=financial_researcher_tools,
        max_iterations=5,
        verbose=True
    )

    # Test with a financial query
    query = "Analyze the financial performance of Siemens AG including P&L and balance sheet"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"\nResult preview: {str(result)[:200]}...")


async def example_web_enhanced_agent():
    """Create an agent with both Fusionbase and web search capabilities."""
    print("\n=== Example 3: Web-Enhanced Agent ===")

    if not os.environ.get("SERP_API_KEY"):
        print("Skipping - SERP_API_KEY not set")
        return

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Define researcher tools with web capabilities
    web_researcher_tools = [
        # Fusionbase tools
        organization_search,
        organization_detail,
        financial_kpi,
        news,

        # Web tools
        google_search,
        web_content,
    ]

    # Create agent with web capabilities
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=web_researcher_tools,
        serp_api_key=os.environ.get("SERP_API_KEY"),
        max_iterations=7,
        verbose=True
    )

    # Test with a query requiring web search
    query = "Find recent news about Tesla Inc and their latest financial performance"
    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    print(f"\nResult preview: {str(result)[:200]}...")


async def main():
    """Run all examples."""
    print("Custom Tools Configuration Examples")
    print("===================================")

    # Run examples
    await example_minimal_agent()
    await example_financial_focused_agent()
    await example_web_enhanced_agent()

    print("\n✅ All examples completed!")
    print("\nKey takeaways:")
    print("- You can customize which tools the researcher has access to")
    print("- Always include GlobalFinding and ResearchReflection for output")
    print("- Tools requiring credentials (like Fusionbase tools) are handled automatically")
    print("- Mix and match tools based on your use case")


if __name__ == "__main__":
    asyncio.run(main())
