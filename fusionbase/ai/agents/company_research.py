"""Company research agent using LangChain, OpenAI, and Fusionbase tools."""

import asyncio
import os
from typing import Any, Dict, List, Optional, TypedDict

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
from .prompts import SYNTHESIS_INSTRUCTIONS

# pylint: disable=line-too-long,too-many-arguments,too-many-locals,too-many-branches,too-many-statements
# pylint: disable=too-many-nested-blocks,unused-variable,redefined-outer-name,too-many-positional-arguments





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

class ResearchPlan(BaseModel):
    """A plan for researching the topic."""
    key_questions: List[str]
    research_areas: List[str]
    information_sources: List[str]

@tool
class Plan(BaseModel):
    """Define a research plan for the query."""
    key_questions: List[str] = Field(
        description="The key questions that need to be answered to fulfill the research goal."
    )
    research_areas: List[str] = Field(
        description="Specific areas to investigate during research."
    )
    information_sources: List[str] = Field(
        description="Potential sources of information to consult."
    )

def create_company_research_agent(
    fusionbase_client: Fusionbase,
    supervisor_model: str = "gpt-4.1",
    researcher_model: str = "gpt-4.1",
    serp_api_key: str = None,
    max_iterations: int = 10,
    verbose: bool = False,
    proxies: Optional[Dict[str, str]] = None,
    verify_ssl: bool = True
):
    """Create a company research agent using direct implementation.

    Args:
        fusionbase_client: Client for accessing Fusionbase data
        supervisor_model: Model to use for the supervisor agent
        researcher_model: Model to use for the researcher agent
        serp_api_key: API key for SERP web search
        max_iterations: Maximum iterations before forcing completion
        verbose: Enable verbose output
        proxies: Optional dictionary of proxies to use (e.g., {"http": "http://proxy:8080", "https": "https://proxy:8080"})
        verify_ssl: Whether to verify SSL certificates (set to False when using certain proxies)
    """

    if not fusionbase_client:
        raise ValueError("A valid Fusionbase client is required")

    # Initialize the client
    fb_client = fusionbase_client

    # Get SERP API key
    if not serp_api_key:
        serp_api_key = os.environ.get("SERP_API_KEY")

    # Define tools for different agent roles
    planner_tools = [
        organization_search,
        google_search,
        Plan
    ]

    researcher_tools = [
        organization_search,
        organization_detail,
        google_search,
        web_content,
        Section,
        Queries
    ]

    synthesizer_tools = [
        Introduction,
        Conclusion
    ]

    # Create tool maps - fix variable name for consistency
    planner_tool_map = {tool.name: tool for tool in planner_tools}
    researcher_tool_map = {tool.name: tool for tool in researcher_tools}  # Changed from research_tool_map to researcher_tool_map
    synthesizer_tool_map = {tool.name: tool for tool in synthesizer_tools}

    # Bind tools to models with different temperatures
    planner_model = ChatOpenAI(model=supervisor_model, temperature=0).bind_tools(planner_tools)
    researcher_model = ChatOpenAI(model=researcher_model, temperature=0).bind_tools(researcher_tools)
    synthesizer_model = ChatOpenAI(model=supervisor_model, temperature=0.2).bind_tools(synthesizer_tools)

    async def create_research_plan(company_topic: str, query: str) -> ResearchPlan:
        """Create a structured research plan for the query."""
        if verbose:
            print(f"\n📋 Creating research plan for: '{query}'")

        messages = [
            SystemMessage(content=(
                "You are a strategic research planner. Your job is to analyze a research question "
                "and create a detailed plan for investigating it. Focus on breaking down complex "
                "questions into manageable parts."
            )),
            HumanMessage(content=f"Create a research plan for the following query about {company_topic}: {query}")
        ]

        research_plan = None
        iterations = 0

        while iterations < 3 and not research_plan:  # Limit planning iterations
            iterations += 1

            if verbose:
                print(f"  ↪ Planning iteration {iterations}/3")

            try:
                response = await planner_model.ainvoke(messages)
                messages.append(response)

                # Extract plan if tool was called
                if hasattr(response, "tool_calls") and response.tool_calls:
                    for tool_call in response.tool_calls:
                        if tool_call["name"] == "Plan":
                            tool_args = dict(tool_call["args"])
                            research_plan = ResearchPlan(
                                key_questions=tool_args.get("key_questions", []),
                                research_areas=tool_args.get("research_areas", []),
                                information_sources=tool_args.get("information_sources", [])
                            )

                            if verbose:
                                print("  ✅ Research plan created")
                                print(f"    📌 Key questions: {len(research_plan.key_questions)}")
                                print(f"    🔍 Research areas: {len(research_plan.research_areas)}")
                                print(f"    📚 Information sources: {len(research_plan.information_sources)}")
                            break

                # If no plan tool was called, prompt directly
                if not research_plan:
                    messages.append(HumanMessage(content=(
                        "Please use the Plan tool to create a structured research plan. "
                        "We need key_questions, research_areas, and information_sources."
                    )))
            except Exception as e:
                if verbose:
                    print(f"  ❌ Error in planning: {str(e)}")

        # Create fallback plan if needed
        if not research_plan:
            if verbose:
                print("  ⚠️ Using fallback research plan")

            research_plan = ResearchPlan(
                key_questions=[f"What information can be found about {company_topic}?",
                              f"What are the main details about {company_topic}?"],
                research_areas=["Company information", "Industry details"],
                information_sources=["Fusionbase database", "Company website", "Web search"]
            )

        return research_plan

    async def research_area(company_topic: str, area_description: str, context: str = "") -> Dict[str, Any]:
        """Research a specific area related to the company."""
        if verbose:
            print(f"\n📝 [RESEARCH] Investigating: '{area_description[:50]}...'")

        # Build context from plan and any previous findings
        prompt = f"Research the following area for {company_topic}: {area_description}"
        if context:
            prompt += f"\n\nContext from previous research:\n{context}"

        # Initialize conversation
        messages = [
            SystemMessage(content=RESEARCH_INSTRUCTIONS.format(section_description=area_description)),
            HumanMessage(content=prompt)
        ]

        # Research variables
        findings = {}
        iterations = 0
        last_response = ""

        while iterations < max_iterations:
            iterations += 1

            if verbose:
                print(f"  ↪ Research iteration {iterations}/{max_iterations}")

            try:
                response = await researcher_model.ainvoke(messages)
                messages.append(response)
                last_response = response.content if hasattr(response, "content") else ""

                # Process tool calls - fix the variable reference
                if hasattr(response, "tool_calls") and response.tool_calls:
                    for tool_call in response.tool_calls:
                        tool_name = tool_call["name"]
                        tool_call_id = tool_call["id"]
                        tool_args = dict(tool_call["args"])

                        if verbose:
                            arg_str = ", ".join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}"
                                              for k, v in tool_args.items() if k not in ("client", "api_key")])
                            print(f"  🔧 Tool: {tool_name}({arg_str})")

                        try:
                            # Add appropriate credentials and proxies
                            if tool_name in ("organization_search", "organization_detail"):
                                tool_args["client"] = fb_client
                            elif tool_name == "google_search":
                                tool_args["api_key"] = serp_api_key
                                if proxies:
                                    tool_args["proxies"] = proxies
                                tool_args["verify_ssl"] = verify_ssl
                            elif tool_name == "web_content" and proxies:
                                tool_args["proxies"] = proxies
                                tool_args["verify_ssl"] = verify_ssl

                            # Execute tool - Fix the variable name here
                            tool = researcher_tool_map[tool_name]  # Changed from research_tool_map to researcher_tool_map
                            result = await tool.ainvoke(tool_args) if hasattr(tool, "ainvoke") else tool.invoke(tool_args)

                            # Save Section results to findings
                            if tool_name == "Section":
                                findings[tool_args.get("name", "Untitled")] = tool_args.get("content", "")
                                if verbose:
                                    print(f"    ✅ Saved finding: '{tool_args.get('name', 'Untitled')}'")

                            # Add tool message to conversation
                            messages.append(ToolMessage(
                                content=str(result),
                                name=tool_name,
                                tool_call_id=tool_call_id
                            ))

                        except Exception as e:
                            error_msg = f"Error executing {tool_name}: {str(e)}"
                            messages.append(ToolMessage(
                                content=error_msg,
                                name=tool_name,
                                tool_call_id=tool_call_id
                            ))
                            if verbose:
                                print(f"    ❌ Error: {str(e)}")
                else:
                    # No more tool calls - done with this research area
                    if verbose:
                        print("  ✓ Research area complete")
                    break

            except Exception as e:
                if verbose:
                    print(f"  ❌ Research error: {str(e)}")
                break

        # Save the last response as a finding if no sections were created
        if not findings and last_response:
            findings["Summary"] = last_response

        return {
            "area": area_description,
            "findings": findings,
            "iterations": iterations,
            "last_response": last_response
        }

    async def synthesize_findings(company_topic: str, query: str, research_plan, research_results) -> str:
        """Synthesize all research findings into a cohesive answer."""
        if verbose:
            print("\n🔄 Synthesizing findings into final response")

        # Extract findings from research results
        all_findings = {}
        for result in research_results:
            all_findings.update(result.get("findings", {}))

        # No findings - return error message
        if not all_findings:
            if verbose:
                print("  ⚠️ No findings to synthesize")
            return f"# Research on {company_topic}\n\nUnable to find specific information about {company_topic} related to your query."

        # Prepare context with research plan and findings
        context = [
            f"## Research Query\n{query}",
            "## Research Plan",
            "Key Questions:",
            *[f"- {q}" for q in research_plan.key_questions],
            "Research Areas:",
            *[f"- {area}" for area in research_plan.research_areas],
            "## Research Findings"
        ]

        # Add all findings to context
        for section, content in all_findings.items():
            context.append(f"### {section}")
            context.append(content)

        context_text = "\n\n".join(context)

        # Synthesize findings
        messages = [
            SystemMessage(content=SYNTHESIS_INSTRUCTIONS),
            HumanMessage(content=f"Synthesize the following research findings about {company_topic} "
                                f"to answer the query: '{query}'\n\n{context_text}")
        ]

        intro_content = None
        conclusion_content = None
        final_report = ""
        iterations = 0

        while iterations < 3 and not final_report:  # Limit synthesis iterations
            iterations += 1

            if verbose:
                print(f"  ↪ Synthesis iteration {iterations}/3")

            try:
                response = await synthesizer_model.ainvoke(messages)
                messages.append(response)

                # Process tool calls for introduction and conclusion
                if hasattr(response, "tool_calls") and response.tool_calls:
                    for tool_call in response.tool_calls:
                        tool_name = tool_call["name"]
                        tool_call_id = tool_call["id"]
                        tool_args = dict(tool_call["args"])

                        if verbose:
                            print(f"  🔧 Using: {tool_name}({tool_args.get('name', 'Untitled')})")

                        try:
                            tool = synthesizer_tool_map[tool_name]
                            result = await tool.ainvoke(tool_args) if hasattr(tool, "ainvoke") else tool.invoke(tool_args)

                            if tool_name == "Introduction":
                                intro_content = f"# {result.name}\n\n{result.content}"
                                if verbose:
                                    print(f"  ✅ Introduction created: '{result.name}'")

                            elif tool_name == "Conclusion":
                                conclusion_content = f"## {result.name}\n\n{result.content}"
                                if verbose:
                                    print(f"  ✅ Conclusion created: '{result.name}'")

                            # Add tool message to conversation
                            messages.append(ToolMessage(
                                content=str(result),
                                name=tool_name,
                                tool_call_id=tool_call_id
                            ))

                        except Exception as e:
                            if verbose:
                                print(f"  ❌ Error in synthesis: {str(e)}")
                else:
                    # No more tool calls - try to use response directly
                    if not intro_content and not conclusion_content and response.content:
                        final_report = response.content
                        if verbose:
                            print("  ✅ Created direct response without tools")
                        break

                # If we have both intro and conclusion, create the report
                if intro_content and conclusion_content:
                    # Create body content from findings
                    body_parts = []
                    for section, content in all_findings.items():
                        body_parts.append(f"## {section}\n\n{content}")

                    body_content = "\n\n".join(body_parts)
                    final_report = f"{intro_content}\n\n{body_content}\n\n{conclusion_content}"

                    if verbose:
                        print("  ✅ Final report assembled with introduction and conclusion")
                    break

                # Prompt for missing parts
                if not intro_content and not conclusion_content:
                    messages.append(HumanMessage(content="Please use the Introduction and Conclusion tools to create a structured response."))
                elif not intro_content:
                    messages.append(HumanMessage(content="Please use the Introduction tool to create a proper introduction."))
                elif not conclusion_content:
                    messages.append(HumanMessage(content="Please use the Conclusion tool to create a proper conclusion."))

            except Exception as e:
                if verbose:
                    print(f"  ❌ Synthesis error: {str(e)}")
                break

        # If we still don't have a report, create a simple one
        if not final_report:
            if verbose:
                print("  ⚠️ Creating fallback report from findings")

            # Create a simple report structure
            parts = [f"# Research on {company_topic}\n\n"]

            if intro_content:
                parts.append(intro_content)
            else:
                parts.append(f"## Overview\n\nThis report presents findings about {company_topic} related to: {query}")

            for section, content in all_findings.items():
                parts.append(f"## {section}\n\n{content}")

            if conclusion_content:
                parts.append(conclusion_content)
            else:
                # Extract a simple conclusion from the last response
                if response and hasattr(response, "content") and response.content:
                    parts.append(f"## Conclusion\n\n{response.content}")
                else:
                    parts.append("## Conclusion\n\nResearch completed with the findings presented above.")

            final_report = "\n\n".join(parts)

        return final_report

    async def ainvoke(initial_state):
        """Execute the research agent process."""
        # Extract the query from the initial state
        user_messages = initial_state.get("messages", [])

        if not user_messages:
            raise ValueError("No query provided in the initial state 'messages' field")

        # Get the user query from messages
        if isinstance(user_messages[0], dict):
            user_query = user_messages[0].get("content", "")
        elif hasattr(user_messages[0], "content"):
            user_query = user_messages[0].content
        else:
            raise ValueError("Invalid message format in initial state")

        if not user_query:
            raise ValueError("Empty query provided")

        # Extract company_topic from the query if not provided explicitly
        company_topic = initial_state.get("company_topic")
        if not company_topic:
            # Try to extract a company or topic from the query
            # This will be refined during the planning phase
            company_topic = "the requested topic"  # Generic placeholder

            # Look for potential company or topic indicators in the query
            if "about " in user_query:
                topic_start = user_query.find("about ") + 6
                topic_end = user_query.find(" ", topic_start)
                if topic_end > topic_start:
                    company_topic = user_query[topic_start:topic_end]
            elif "for " in user_query:
                topic_start = user_query.find("for ") + 4
                topic_end = user_query.find(" ", topic_start)
                if topic_end > topic_start:
                    company_topic = user_query[topic_start:topic_end]

        if verbose:
            print(f"\n🚀 Starting research process")
            print(f"🔎 Query: '{user_query}'")
            print(f"🔄 Using models: Plan/Synthesis={supervisor_model}, Research={researcher_model}")

        try:
            # PHASE 1: PLANNING
            research_plan = await create_research_plan(company_topic, user_query)

            # PHASE 2: RESEARCH
            if verbose:
                print(f"\n🔍 Executing research plan with {len(research_plan.research_areas)} areas")

            # Convert research areas to actual research tasks
            if not research_plan.research_areas:
                research_plan.research_areas = ["Company information and details"]

            # Execute research for each area
            research_tasks = [
                research_area(company_topic, area) for area in research_plan.research_areas
            ]
            research_results = await asyncio.gather(*research_tasks)

            # PHASE 3: SYNTHESIS
            final_report = await synthesize_findings(company_topic, user_query, research_plan, research_results)

            if verbose:
                print("\n🏁 Research process complete")
                print(f"📄 Report generated ({len(final_report)} chars)")

            return {"final_report": final_report}

        except Exception as e:
            import traceback
            if verbose:
                print(f"\n❌ Error in research process: {str(e)}")
                traceback.print_exc()

            return {
                "final_report": f"# Research Error\n\nAn error occurred while researching {company_topic}: {str(e)}"
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

def extract_url_from_text(text):
    """Extract a URL from text."""
    import re  # pylint: disable=import-outside-toplevel
    urls = re.findall(r'https?://[^\s]+', text)
    if urls and "linkedin.com" in urls[0]:
        # Clean up URL (remove trailing punctuation)
        url = urls[0]
        return url.rstrip('.,"\')')
    return None

def create_direct_answer_report(company_topic, facts, query):
    """Create a report for direct answer queries."""
    report = f"# Information about {company_topic}\n\n"

    # Add facts with headers
    for key, value in facts.items():
        report += f"## {key}\n"
        if isinstance(value, list):
            report += "\n".join(value) + "\n\n"
        else:
            report += f"{value}\n\n"

    # If we have LinkedIn info, make it prominent
    if "LinkedIn URL" in facts:
        linkedin = facts["LinkedIn URL"]
        if "linkedin.com" in linkedin and not linkedin.startswith("http"):
            linkedin = "https://" + linkedin
        report = report.replace("## LinkedIn URL", f"## LinkedIn URL\n[{company_topic} on LinkedIn]({linkedin})")

    # Add a conclusion if NAICS info was requested but not found
    if "naics" in query.lower() and "NAICS Information" not in facts:
        report += "\n## NAICS Code\nNo specific NAICS code information was found for this company."
        report += "\nBased on the company's activities in data technology and management, potential NAICS codes could include:"
        report += "\n- 518210: Data Processing, Hosting, and Related Services"
        report += "\n- 541512: Computer Systems Design Services"
        report += "\n- 511210: Software Publishers"

    return report

def create_fallback_report(messages, company_topic, query):
    """Create a fallback report from conversation history."""
    # Extract useful information from messages
    useful_content = []
    for msg in messages:
        if hasattr(msg, "content") and isinstance(msg.content, str):
            content = msg.content.strip()
            # Ignore system messages, short messages, and tool calls
            if len(content) > 100 and not content.startswith("You are") and "tool_call" not in content:
                useful_content.append(content)

    # Use the most recent substantial content
    if useful_content:
        return f"# Information about {company_topic}\n\n{useful_content[-1]}"

    return f"# Information about {company_topic}\n\nNo specific information could be found for the query: {query}"
