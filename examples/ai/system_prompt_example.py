#!/usr/bin/env python
"""System Prompt Example

This example shows how to pass system prompts through the messages array
instead of as a constructor argument. This is the standard way to provide
system instructions to the agent.
"""

import asyncio
import os
import sys
from typing import Any, Dict

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


async def example_with_system_prompt():
    """Example showing how to use system prompts in messages."""
    print("System Prompt Example")
    print("=" * 50)

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Create agent without system_prompt parameter
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=ToolPresets.minimal_researcher_tools(),
        max_iterations=3,
        verbose=True
    )

    # Pass system prompt in messages array
    result = await agent.ainvoke({
        "messages": [
            {
                "role": "system",
                "content": "Focus on sustainability and ESG aspects when researching companies. "
                          "Always highlight environmental initiatives and social responsibility programs."
            },
            {
                "role": "user",
                "content": "Research BMW AG"
            }
        ]
    })

    print("\n" + "=" * 50)
    print("RESULT")
    print("=" * 50)
    print(result[:1000] + "..." if len(str(result)) > 1000 else result)


async def example_data_enrichment():
    """Example showing intent-based token efficiency."""
    print("\n\nData Enrichment Example")
    print("=" * 50)

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Import the new convenience tools
    from fusionbase.ai.tools import organization_detail
    from fusionbase.ai.tools import organization_search
    from fusionbase.ai.tools.relation.convenience_v2 import balance_sheet_accounts
    from fusionbase.ai.tools.relation.convenience_v2 import financial_kpi

    # Create agent with financial tools
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=[
            organization_search,
            organization_detail,
            financial_kpi,
            balance_sheet_accounts
        ],
        max_iterations=2,
        verbose=False
    )

    # System prompt guides the agent on efficient data handling
    result = await agent.ainvoke({
        "messages": [
            {
                "role": "system",
                "content": "When the user asks to 'get' or 'fetch' data without requesting analysis, "
                          "use return_mode='placeholder' for efficient data retrieval. "
                          "When analysis or interpretation is needed, use return_mode='full'."
            },
            {
                "role": "user",
                "content": "Get the financial KPIs and balance sheet for SAP SE"
            }
        ]
    })

    print("\nThe agent should use placeholder mode for efficient data retrieval.")
    print(f"Result type: {type(result)}")
    print(f"Result preview: {str(result)[:200]}...")


async def example_analysis_request():
    """Example showing when full data is needed."""
    print("\n\nAnalysis Request Example")
    print("=" * 50)

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=ToolPresets.financial_researcher_tools(),
        max_iterations=3,
        verbose=False
    )

    # Request that requires analysis
    result = await agent.ainvoke({
        "messages": [
            {
                "role": "system",
                "content": "Provide insightful analysis, not just raw data. "
                          "Focus on trends, comparisons, and meaningful interpretations."
            },
            {
                "role": "user",
                "content": "Analyze the financial health of Volkswagen AG based on their latest KPIs"
            }
        ]
    })

    print("\nThe agent should use full mode to analyze the data.")
    print(f"Result preview: {str(result)[:500]}...")


async def main():
    """Run all examples."""
    print("Company Research Agent - System Prompt Examples")
    print("=" * 80)
    print("\nThis demonstrates the new message-based system prompt approach:")
    print("- System prompts are passed in the messages array with role='system'")
    print("- No system_prompt parameter in the constructor")
    print("- Supports multiple message types in a single invocation")
    print("- Intent-based token efficiency based on user queries\n")

    await example_with_system_prompt()
    await example_data_enrichment()
    await example_analysis_request()

    print("\n\n✅ All examples completed!")
    print("\nKey Takeaways:")
    print("1. Pass system prompts in messages with role='system'")
    print("2. System prompts guide agent behavior and tool usage")
    print("3. Tools automatically choose efficient mode based on intent")
    print("4. No configuration needed for token efficiency")


if __name__ == "__main__":
    asyncio.run(main())
