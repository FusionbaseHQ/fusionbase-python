#!/usr/bin/env python
"""Structured Output Example: Personnel Extraction

This example shows how to use structured output with Pydantic schemas to extract
company personnel information. Instead of getting a markdown report, you get
typed, validated data that's ready for:
- Database storage
- API responses
- Further processing

The example extracts executives, board members, and former personnel into a
well-defined schema.
"""

import asyncio
from datetime import datetime
import json
import os
import sys
from typing import List, Optional

# Check for required environment variables
if not os.environ.get("OPENAI_API_KEY"):
    print("Error: OPENAI_API_KEY environment variable not set.")
    sys.exit(1)

if not os.environ.get("FUSIONBASE_API_KEY"):
    print("Error: FUSIONBASE_API_KEY environment variable not set.")
    sys.exit(1)

from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from pydantic import Field

from fusionbase import Fusionbase
from fusionbase.ai.agents import create_company_research_agent
from fusionbase.ai.tools import network
from fusionbase.ai.tools import organization_detail
from fusionbase.ai.tools import organization_search
from fusionbase.ai.tools import publications
from fusionbase.ai.tools import related_persons

# Internal tools are now automatically included by the agent


# Define the schema for person information
class Person(BaseModel):
    """Information about a person associated with the company."""
    name: str = Field(description="Full name of the person")
    position: Optional[str] = Field(description="Current or former position/title")
    role_type: Optional[str] = Field(description="Type of role (Executive, Board Member, etc.)")
    status: Optional[str] = Field(description="Current or Former")
    start_date: Optional[str] = Field(default=None, description="When they started")
    end_date: Optional[str] = Field(default=None, description="When they left (if applicable)")


class CompanyPersonnel(BaseModel):
    """All personnel information for a company."""
    company_name: str = Field(description="Name of the company")
    entity_id: Optional[str] = Field(description="Fusionbase entity ID")
    extraction_timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    current_executives: List[Person] = Field(
        default_factory=list,
        description="Current executive team members"
    )
    current_board_members: List[Person] = Field(
        default_factory=list,
        description="Current board members"
    )
    former_personnel: List[Person] = Field(
        default_factory=list,
        description="Former executives and board members"
    )

    total_persons_found: int = Field(description="Total number of persons found")
    data_sources: List[str] = Field(
        default_factory=list,
        description="Where this information came from"
    )


async def extract_fusionbase_personnel():
    """Extract all personnel for Fusionbase GmbH."""
    print("Extracting Personnel for Fusionbase GmbH")
    print("=" * 50)

    # Initialize clients
    fb_client = Fusionbase()
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Configure tools specifically for finding people
    personnel_tools = [
        # Core tools
        organization_search,
        organization_detail,

        # Person-specific tools
        related_persons,  # Direct person relations
        publications,     # Often contains appointments/resignations
        network,         # May show key people in subsidiaries

        # Note: Internal tools (GlobalFinding, ResearchReflection) are automatically included
    ]

    # Create agent with structured output
    print("\nConfiguring agent with:")
    print(f"- {len(personnel_tools)} specialized tools")
    print("- Structured output schema: CompanyPersonnel")
    print("- Target: Fusionbase GmbH\n")

    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        researcher_tools=personnel_tools,
        output_schema=CompanyPersonnel,  # This ensures structured output
        max_iterations=5,
        verbose=True
    )

    # Run the extraction
    query = """Find all personnel information for Fusionbase GmbH including:
    - Current executives and management
    - Board members
    - Former key personnel
    Extract names, positions, and any available dates."""

    print(f"Query: {query}\n")
    print("-" * 50)

    result = await agent.ainvoke({
        "messages": [{"role": "user", "content": query}]
    })

    # Process the structured output
    personnel_data = result

    print("\n" + "=" * 50)
    print("EXTRACTION RESULTS")
    print("=" * 50)

    if isinstance(personnel_data, dict):
        # If it's a dict, it was successfully structured
        print("\n✅ Successfully extracted structured data\n")

        # Pretty print the results
        print(json.dumps(personnel_data, indent=2, ensure_ascii=False))

        # Summary statistics
        total = personnel_data.get("total_persons_found", 0)
        print(f"\n📊 Summary:")
        print(f"   Total persons found: {total}")
        print(f"   Current executives: {len(personnel_data.get('current_executives', []))}")
        print(f"   Current board members: {len(personnel_data.get('current_board_members', []))}")
        print(f"   Former personnel: {len(personnel_data.get('former_personnel', []))}")

        # Save to file
        output_file = "fusionbase_personnel.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(personnel_data, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Results saved to: {output_file}")

    elif hasattr(personnel_data, 'model_dump'):
        # If it's a Pydantic model
        print("\n✅ Successfully extracted structured data (Pydantic)\n")
        data_dict = personnel_data.model_dump()
        print(json.dumps(data_dict, indent=2, ensure_ascii=False))

        # Save to file
        output_file = "fusionbase_personnel.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Results saved to: {output_file}")

    else:
        # Fallback: received markdown
        print("\n⚠️ Received unstructured output (markdown):")
        print(personnel_data[:1000] + "..." if len(str(personnel_data)) > 1000 else personnel_data)


async def main():
    """Run the personnel extraction."""
    try:
        await extract_fusionbase_personnel()
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("Company Personnel Extractor")
    print("==========================")
    print("This tool extracts structured personnel data for companies.")
    print("Using all available tools to find comprehensive information.\n")

    asyncio.run(main())
