#!/usr/bin/env python
"""Output Formats Comparison

This example demonstrates the difference between:
1. Traditional markdown reports (human-readable)
2. Structured JSON/Pydantic output (machine-readable)

Run this to see the same query processed in both formats and understand
when to use each approach.
"""

import asyncio
import json
import os
from typing import List, Optional

from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from pydantic import Field

from fusionbase import Fusionbase
from fusionbase.ai.agents import create_company_research_agent
from fusionbase.ai.agents import ToolPresets


class CompanyQuickInfo(BaseModel):
    """Simple structured format for company information."""
    company_name: str = Field(description="Official company name")
    industry: Optional[str] = Field(description="Primary industry", default=None)
    headquarters: Optional[str] = Field(description="Headquarters location", default=None)
    website: Optional[str] = Field(description="Company website", default=None)
    employee_count: Optional[int] = Field(description="Number of employees", default=None)
    key_products: List[str] = Field(description="Main products or services", default_factory=list)
    summary: str = Field(description="Brief company description")


async def get_markdown_output(company: str):
    """Get traditional markdown output."""
    print(f"\n{'='*60}")
    print(f"MARKDOWN OUTPUT for {company}")
    print('='*60)

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Create agent WITHOUT output_schema
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

    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": f"Provide a brief overview of {company}"}]
    })

    markdown = result
    print("\n📄 Markdown Report:")
    print("-" * 40)
    print(markdown[:500] + "..." if len(markdown) > 500 else markdown)

    return markdown


async def get_structured_output(company: str):
    """Get structured output."""
    print(f"\n\n{'='*60}")
    print(f"STRUCTURED OUTPUT for {company}")
    print('='*60)

    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Create agent WITH output_schema
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=ToolPresets.minimal_researcher_tools(),
        output_schema=CompanyQuickInfo,  # This triggers structured output
        max_iterations=3,
        verbose=False
    )

    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": f"Provide a brief overview of {company}"}]
    })

    structured = result

    print("\n📊 Structured Data:")
    print("-" * 40)

    if isinstance(structured, dict):
        print(json.dumps(structured, indent=2))
    elif hasattr(structured, 'model_dump'):
        print(json.dumps(structured.model_dump(), indent=2))

    return structured


async def compare_outputs():
    """Compare both output formats for the same company."""
    company = "SAP SE"

    print(f"Comparing Output Formats for: {company}")
    print("=" * 80)

    # Get both outputs
    markdown = await get_markdown_output(company)
    structured = await get_structured_output(company)

    # Show the differences
    print(f"\n\n{'='*60}")
    print("COMPARISON")
    print('='*60)

    print("\n📝 Markdown Output Characteristics:")
    print("- Human-readable narrative format")
    print("- Good for reports and documentation")
    print(f"- Length: {len(markdown)} characters")
    print("- Format: Free-form text with sections")

    print("\n🔧 Structured Output Characteristics:")
    print("- Machine-readable JSON/object format")
    print("- Good for APIs and databases")
    print("- Validated against schema")
    print("- Predictable field names and types")

    if isinstance(structured, (dict, BaseModel)):
        data = structured if isinstance(structured, dict) else structured.model_dump()
        print(f"- Fields: {', '.join(data.keys())}")
        print(f"- Can be directly used in code:")
        print(f"  company_name = data['company_name']  # {data.get('company_name', 'N/A')}")
        if 'employee_count' in data and data['employee_count']:
            print(f"  employees = data['employee_count']    # {data['employee_count']}")


async def main():
    """Run the comparison."""
    print("Structured vs Markdown Output Demo")
    print("==================================")
    print("\nThis demo shows the difference between:")
    print("1. Traditional markdown reports (human-friendly)")
    print("2. Structured data output (machine-friendly)")

    await compare_outputs()

    print("\n\n✅ Demo Complete!")
    print("\nWhen to use each format:")
    print("- Markdown: Reports, emails, documentation, human review")
    print("- Structured: APIs, databases, automation, data pipelines")


if __name__ == "__main__":
    asyncio.run(main())
