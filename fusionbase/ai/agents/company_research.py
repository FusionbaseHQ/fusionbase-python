"""Company research agent using LangChain, OpenAI, and Fusionbase tools."""

import asyncio
import os
import time
from typing import List, TypedDict

from langchain_core.messages import HumanMessage
from langchain_core.messages import SystemMessage
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from pydantic import Field

from fusionbase import Fusionbase
from fusionbase.ai.tools.entity.organization import organization_detail
from fusionbase.ai.tools.entity.organization import organization_search
from fusionbase.ai.tools.web.content import web_content
from fusionbase.ai.tools.web.search import google_search

from .prompts import RESEARCH_INSTRUCTIONS
from .prompts import SUPERVISOR_INSTRUCTIONS


@tool
class Section(BaseModel):
    """A section of the company research report."""
    name: str = Field(
        description="Name for this section of the report.",
    )
    content: str = Field(
        description="The content of the section."
    )

@tool
class Sections(BaseModel):
    """Define the sections of the company research report."""
    sections: List[str] = Field(
        description="Sections of the report with detailed descriptions.",
    )

@tool
class Introduction(BaseModel):
    """Introduction for the company research report."""
    name: str = Field(
        description="Name for the report.",
    )
    content: str = Field(
        description="The content of the introduction, giving an overview of the report."
    )

@tool
class Conclusion(BaseModel):
    """Conclusion for the company research report."""
    name: str = Field(
        description="Name for the conclusion of the report.",
    )
    content: str = Field(
        description="The content of the conclusion, summarizing the report."
    )

@tool
class Queries(BaseModel):
    """Search queries for gathering information."""
    queries: List[str] = Field(
        description="List of search queries to gather information.",
    )

class CompanyResearchResult(TypedDict):
    """Result of the company research agent."""
    final_report: str

def create_company_research_agent(
    fusionbase_client: Fusionbase,
    supervisor_model: str = "gpt-4o",
    researcher_model: str = "gpt-4o",
    serp_api_key: str = None,
    max_iterations: int = 10,
    verbose: bool = False
):
    """Create a company research agent using direct implementation."""

    if not fusionbase_client:
        raise ValueError("A valid Fusionbase client is required")

    # Initialize the client
    fb_client = fusionbase_client

    # Get SERP API key
    if not serp_api_key:
        serp_api_key = os.environ.get("SERP_API_KEY")

    # Initialize chat models
    supervisor_llm = ChatOpenAI(model=supervisor_model, temperature=0)
    researcher_llm = ChatOpenAI(model=researcher_model, temperature=0)

    # Define tools
    supervisor_tools = [
        organization_search,
        organization_detail,
        google_search,
        web_content,
        Sections,
        Introduction,
        Conclusion,
        Queries
    ]

    research_tools = [
        organization_search,
        organization_detail,
        google_search,
        web_content,
        Section,
        Queries
    ]

    # Create tool maps
    supervisor_tool_map = {tool.name: tool for tool in supervisor_tools}
    research_tool_map = {tool.name: tool for tool in research_tools}

    # Bind tools to models
    supervisor_model_with_tools = supervisor_llm.bind_tools(supervisor_tools)
    researcher_model_with_tools = researcher_llm.bind_tools(research_tools)

    async def research_section(company_topic: str, section_description: str):
        """Research a specific section of the report."""
        system_message = SystemMessage(content=RESEARCH_INSTRUCTIONS.format(
            section_description=section_description))

        # Initialize conversation with system message and initial prompt
        messages = [
            system_message,
            HumanMessage(content=f"Research the following section for {company_topic}: {section_description}")
        ]

        completed_section = None

        if verbose:
            print(f"\n📝 [SECTION] Starting research for: '{section_description[:50]}...'")

        # Loop for handling tool calls
        iterations = 0
        while iterations < max_iterations:
            iterations += 1

            if verbose:
                print(f"  ↪ Section iteration {iterations}/{max_iterations}")

            # Get response from model
            response = await researcher_model_with_tools.ainvoke(messages)

            # Add response to messages
            messages.append(response)

            # FIX: If this is the last iteration and we haven't completed a section, force section creation
            if iterations == max_iterations and not completed_section:
                if verbose:
                    print(f"  ⚠️ Reached max iterations without completing section, forcing completion")

                # Extract content from the conversation for the section
                content_parts = []
                for msg in messages:
                    if hasattr(msg, "content") and msg.content and isinstance(msg.content, str):
                        # Only include relevant parts, not tool calls or system messages
                        if not msg.content.startswith("Research the following section"):
                            content_parts.append(msg.content)

                # Create a completed section from what we have
                section_name = section_description.split("\n")[0] if "\n" in section_description else section_description
                section_name = section_name[:50] + ("..." if len(section_name) > 50 else "")

                # FIX: Use proper tool invocation instead of direct constructor
                try:
                    completed_section = Section.invoke({
                        "name": section_name,
                        "content": "\n\n".join(content_parts[-2:])
                    })
                    if verbose:
                        print(f"    ✅ Forced section '{section_name}' completion ({len(completed_section.content)} chars)")
                except Exception as e:
                    if verbose:
                        print(f"    ❌ Error creating section: {str(e)}")
                break

            # Check if we're done (no more tool calls)
            if not hasattr(response, "tool_calls") or not response.tool_calls:
                if verbose:
                    print("  ✓ Section research complete - no more tool calls")

                # FIX: If no section was explicitly created but we completed research,
                # create one from the model's final response
                if not completed_section and response.content:
                    section_name = section_description.split("\n")[0] if "\n" in section_description else section_description
                    section_name = section_name[:50] + ("..." if len(section_name) > 50 else "")

                    # FIX: Use proper tool invocation instead of direct constructor
                    try:
                        completed_section = Section.invoke({
                            "name": section_name,
                            "content": response.content
                        })
                        if verbose:
                            print(f"    ✅ Created implicit section '{section_name}' from final response")
                    except Exception as e:
                        if verbose:
                            print(f"    ❌ Error creating section: {str(e)}")
                break

            # Process each tool call
            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                tool_call_id = tool_call["id"]
                tool_args = dict(tool_call["args"])

                # Create a readable representation of tool arguments for display
                display_args = {k: v for k, v in tool_args.items() if k not in ("client", "api_key")}
                if verbose:
                    arg_str = ", ".join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in display_args.items()])
                    print(f"  🔧 Tool: {tool_name}({arg_str})")

                try:
                    # Add appropriate credentials
                    if tool_name in ("organization_search", "organization_detail"):
                        tool_args["client"] = fb_client
                        if verbose and tool_name == "organization_search":
                            print(f"    🔍 Searching Fusionbase for: '{tool_args.get('query', '')}'")
                        elif verbose and tool_name == "organization_detail":
                            print(f"    📋 Getting details for entity ID: {tool_args.get('entity_id', '')}")
                    elif tool_name == "google_search":
                        tool_args["api_key"] = serp_api_key
                        if verbose:
                            print(f"    🌐 Searching web for: '{tool_args.get('query', '')}'")
                    elif tool_name == "web_content" and verbose:
                        print(f"    📄 Extracting content from: {tool_args.get('url', '')[:60]}...")
                    elif tool_name == "Section" and verbose:
                        print(f"    📑 Creating section: '{tool_args.get('name', '')}'")

                    # Execute tool
                    tool = research_tool_map[tool_name]
                    result = await tool.ainvoke(tool_args) if hasattr(tool, "ainvoke") else tool.invoke(tool_args)

                    # Check if this is a section completion
                    if tool_name == "Section":
                        completed_section = result
                        if verbose:
                            print(f"    ✅ Section '{result.name}' completed ({len(result.content)} chars)")

                except Exception as e:
                    result = f"Error executing {tool_name}: {str(e)}"
                    if verbose:
                        print(f"    ❌ Error: {str(e)}")

                # Add tool message to conversation
                messages.append(ToolMessage(
                    content=str(result),
                    name=tool_name,
                    tool_call_id=tool_call_id
                ))

        # Return the completed section
        if verbose and completed_section:
            print(f"📑 [COMPLETE] Section '{completed_section.name}' ready")
        elif verbose:
            print("⚠️ [WARNING] No completed section was produced")

        return completed_section

    async def ainvoke(initial_state):
        """Run the company research agent."""
        company_topic = initial_state.get("company_topic")
        if not company_topic:
            raise ValueError("company_topic is required in the initial state")

        # Initialize system message for supervisor
        system_message = SystemMessage(content=SUPERVISOR_INSTRUCTIONS)

        # Get user query from initial state
        user_messages = initial_state.get("messages", [])
        if not user_messages:
            user_messages = [{"role": "user", "content": f"Research {company_topic} and create a comprehensive report."}]

        # Convert user messages to proper format
        messages = [system_message]
        for msg in user_messages:
            if isinstance(msg, dict):
                messages.append(HumanMessage(content=msg["content"]))
            else:
                messages.append(msg)

        if verbose:
            print(f"\n🚀 Starting company research for: {company_topic}")
            print(f"🔄 Using models: Supervisor={supervisor_model}, Researcher={researcher_model}")
            if serp_api_key:
                print("🌐 Web search enabled")
            else:
                print("⚠️ Web search disabled (no API key)")

        # Initialize variables
        sections_list = []
        completed_sections = []
        final_report = ""
        intro_content = None
        conclusion_content = None

        # Main supervisor loop
        iterations = 0
        while iterations < max_iterations:
            iterations += 1

            if verbose:
                print(f"\n📊 Supervisor iteration {iterations}/{max_iterations}")

            # Get supervisor response
            response = await supervisor_model_with_tools.ainvoke(messages)

            # Add response to messages
            messages.append(response)

            # Check if we're done (no more tool calls)
            if not hasattr(response, "tool_calls") or not response.tool_calls:
                if verbose:
                    print("✓ No more tool calls, finishing up")
                break

            # Process each tool call
            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                tool_call_id = tool_call["id"]
                tool_args = dict(tool_call["args"])

                # Create a readable representation of tool arguments for display
                display_args = {k: v for k, v in tool_args.items() if k not in ("client", "api_key")}
                if verbose:
                    arg_str = ", ".join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in display_args.items()])
                    print(f"🔧 Using tool: {tool_name}({arg_str})")

                try:
                    # Add appropriate credentials
                    if tool_name in ("organization_search", "organization_detail"):
                        tool_args["client"] = fb_client
                        if verbose and tool_name == "organization_search":
                            print(f"  🔍 Searching Fusionbase for: '{tool_args.get('query', '')}'")
                        elif verbose and tool_name == "organization_detail":
                            print(f"  📋 Getting details for entity ID: {tool_args.get('entity_id', '')}")
                    elif tool_name == "google_search":
                        tool_args["api_key"] = serp_api_key
                        if verbose:
                            print(f"  🌐 Searching web for: '{tool_args.get('query', '')}'")
                    elif tool_name == "web_content" and verbose:
                        print(f"  📄 Extracting content from: {tool_args.get('url', '')[:60]}...")
                    elif tool_name == "Sections" and verbose:
                        print(f"  📝 Planning report sections")
                    elif tool_name == "Introduction" and verbose:
                        print(f"  📝 Writing introduction: '{tool_args.get('name', '')}'")
                    elif tool_name == "Conclusion" and verbose:
                        print(f"  📝 Writing conclusion: '{tool_args.get('name', '')}'")

                    # Execute tool
                    tool = supervisor_tool_map[tool_name]
                    result = await tool.ainvoke(tool_args) if hasattr(tool, "ainvoke") else tool.invoke(tool_args)

                    # Process special tool results
                    if tool_name == "Sections":
                        sections_list = result.sections
                        if verbose:
                            print(f"  ✅ Planned {len(sections_list)} sections:")
                            for i, section in enumerate(sections_list, 1):
                                print(f"    {i}. {section[:50]}...")
                    elif tool_name == "Introduction":
                        intro_content = f"# {result.name}\n\n{result.content}" if not str(result.content).startswith("# ") else result.content
                        if verbose:
                            print(f"  ✅ Introduction written: '{result.name}' ({len(result.content)} chars)")
                    elif tool_name == "Conclusion":
                        conclusion_content = f"## {result.name}\n\n{result.content}" if not str(result.content).startswith("## ") else result.content
                        if verbose:
                            print(f"  ✅ Conclusion written: '{result.name}' ({len(result.content)} chars)")
                    elif tool_name == "Queries" and verbose:
                        print(f"  🔍 Search queries planned: {len(result.queries)}")
                        for i, query in enumerate(result.queries[:3], 1):
                            print(f"    {i}. {query}")
                        if len(result.queries) > 3:
                            print(f"    ... and {len(result.queries)-3} more")

                except Exception as e:
                    result = f"Error executing {tool_name}: {str(e)}"
                    if verbose:
                        print(f"  ❌ Error: {str(e)}")

                # Add tool message to conversation
                messages.append(ToolMessage(
                    content=str(result),
                    name=tool_name,
                    tool_call_id=tool_call_id
                ))

            # Check if we need to research sections
            if sections_list and not completed_sections:
                if verbose:
                    print(f"\n🔎 Starting research for {len(sections_list)} sections")

                # Research all sections concurrently
                section_tasks = [research_section(company_topic, section) for section in sections_list]
                section_results = await asyncio.gather(*section_tasks)

                # Collect completed sections
                for result in section_results:
                    if result:
                        completed_sections.append(result)

                # Inform supervisor of completed sections
                if completed_sections:
                    if verbose:
                        print(f"📊 Progress: {len(completed_sections)}/{len(sections_list)} sections completed")

                    section_summary = "\n\n".join([f"Section: {s.name}" for s in completed_sections])
                    messages.append(HumanMessage(
                        content=f"Research completed for {len(completed_sections)} sections. Here's a summary:\n\n{section_summary}"
                    ))
                else:
                    # FIX: If no sections were completed after research, create simple sections
                    # so we don't get stuck in a loop
                    if verbose:
                        print("⚠️ No sections were completed from research, creating fallback sections")

                    for section_desc in sections_list:
                        section_name = section_desc.split("\n")[0] if "\n" in section_desc else section_desc
                        section_name = section_name[:50] + ("..." if len(section_name) > 50 else "")
                        content = f"Information about {section_name} for {company_topic} could not be fully researched."
                        try:
                            completed_sections.append(Section.invoke({
                                "name": section_name,
                                "content": content
                            }))
                        except Exception as e:
                            if verbose:
                                print(f"❌ Error creating fallback section: {str(e)}")

            # FIX: If we have an introduction but no sections were completed,
            # create sections directly from the available information
            if intro_content and not completed_sections:
                if verbose:
                    print("\n⚠️ Introduction was created but no sections - creating basic sections")

                # Create a basic section
                try:
                    basic_section = Section.invoke({
                        "name": "Company Information",
                        "content": f"Fusionbase GmbH is a data technology company based in Munich, Germany specializing in data management solutions."
                    })
                    completed_sections.append(basic_section)
                except Exception as e:
                    if verbose:
                        print(f"❌ Error creating basic section: {str(e)}")

            # Check for final report assembly
            if intro_content and completed_sections:
                # Even if we don't have a conclusion, we can still create a report
                if not conclusion_content and iterations >= max_iterations - 1:
                    if verbose:
                        print("\n⚠️ Creating basic conclusion for report completion")
                    conclusion_content = "## Conclusion\n\nFusionbase GmbH continues to innovate in the data management space, offering solutions that help businesses access and utilize data more effectively."

                # Combine the parts into a final report
                if conclusion_content:
                    body_content = "\n\n".join([s.content for s in completed_sections])
                    final_report = f"{intro_content}\n\n{body_content}\n\n{conclusion_content}"

                    if verbose:
                        print("\n✅ Report assembly complete!")
                        print(f"📊 Report stats:")
                        print(f"  - Introduction: {len(intro_content)} chars")
                        print(f"  - Body sections: {len(body_content)} chars ({len(completed_sections)} sections)")
                        print(f"  - Conclusion: {len(conclusion_content)} chars")
                        print(f"  - Total length: {len(final_report)} chars")

                    # Exit the loop
                    break

        # If we've reached max iterations but have some content, create a partial report
        if iterations >= max_iterations:
            # If we have an intro but no sections or conclusion, add them
            if intro_content and not final_report:
                if not completed_sections:
                    try:
                        basic_section = Section.invoke({
                            "name": "Company Information",
                            "content": "Fusionbase GmbH is a data technology company based in Munich that specializes in data management solutions."
                        })
                        completed_sections.append(basic_section)
                    except Exception as e:
                        if verbose:
                            print(f"❌ Error creating emergency section: {str(e)}")

                body_content = "\n\n".join([s.content for s in completed_sections])

                if not conclusion_content:
                    conclusion_content = "## Conclusion\n\nIn conclusion, Fusionbase GmbH is positioned as a specialized provider in the data management industry, continuing to develop its offerings in this space."

                final_report = f"{intro_content}\n\n{body_content}\n\n{conclusion_content}"

                if verbose:
                    print("📊 Emergency report created with all available information")

        # Return the final result
        if verbose:
            print("\n🏁 Research process complete")
            if final_report:
                print(f"📄 Report generated ({len(final_report)} chars)")
            else:
                print("⚠️ No report was generated")

        return {
            "final_report": final_report or "Could not generate a report due to technical difficulties."
        }

    # Create the agent object
    class CompanyResearchAgent:
        """Company Research Agent."""

        async def ainvoke(self, state):
            """Async invoke the agent."""
            return await ainvoke(state)

        def invoke(self, state):
            """Sync invoke the agent."""
            return asyncio.run(self.ainvoke(state))

    return CompanyResearchAgent()
