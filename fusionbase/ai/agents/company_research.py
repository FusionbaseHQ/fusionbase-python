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
from fusionbase.ai.tools.entity.relation import relation_detail
from fusionbase.ai.tools.entity.relation import relation_resolve
from fusionbase.ai.tools.entity.relation import relation_search
from fusionbase.ai.tools.web.content import web_content
from fusionbase.ai.tools.web.search import google_search

from .prompts import HALLUCINATION_GRADING_INSTRUCTIONS
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

@tool
class FinalAnswer(BaseModel):
    """Final answer formatted according to user's requirements."""
    content: str = Field(
        description="The final answer formatted exactly as requested by the user"
    )
    format_type: str = Field(
        description="The format type detected (e.g., 'json', 'markdown', 'list', 'plain_text')"
    )

class CompanyResearchResult(TypedDict):
    """Result of the company research agent."""
    final_report: str

class ResearchContext(BaseModel):
    """Global research context shared across all agents."""
    original_query: str
    research_goal: str
    target_entity: str
    research_plan: Optional["ResearchPlan"] = None
    findings_registry: Dict[str, str] = {}  # section_name -> content
    completed_searches: List[str] = []  # Track what's been searched
    key_insights: List[str] = []  # Store important discoveries

class ResearchPlan(BaseModel):
    """A plan for researching the topic."""
    key_questions: List[str]  # Questions that need to be answered
    data_points: List[str]    # Specific data points to gather
    search_strategies: List[str]  # Strategies for finding information

@tool
class Plan(BaseModel):
    """Define a research plan for the query."""
    key_questions: List[str] = Field(
        description="Key questions that need to be answered to fulfill the research goal (e.g., 'What is the company's main business?', 'Who are the key executives?')"
    )
    data_points: List[str] = Field(
        description="Specific data points to gather about the company (e.g., 'LinkedIn URL', 'Website URL', 'Number of employees', 'Founding date', 'CEO name')"
    )
    search_strategies: List[str] = Field(
        description="Deep search strategies to use for comprehensive information gathering (e.g., 'Deep web search with multiple keyword variations', 'Social media profile searches', 'Industry database lookups', 'News article mining', 'Company filing searches')"
    )

@tool
class GlobalFinding(BaseModel):
    """Store a research finding in the global registry."""
    section_name: str = Field(
        description="Name/category of this finding (e.g., 'Company Overview', 'LinkedIn URL', 'Financial Data')"
    )
    content: str = Field(
        description="The research finding content"
    )
    relevance_score: float = Field(
        description="How relevant this finding is to the research goal (0.0-1.0)",
        ge=0.0,
        le=1.0
    )
    sources: List[str] = Field(
        description="Sources where this information was found",
        default_factory=list
    )

def create_company_research_agent(
    fusionbase_client: Fusionbase,
    planner_model: Any,
    researcher_model: Any,
    synthesizer_model: Any,
    hallucination_grader_model: Any,
    serp_api_key: str = None,
    max_iterations: int = 10,
    verbose: bool = False,
    proxies: Optional[Dict[str, str]] = None,
    verify_ssl: bool = True
):
    """Create a company research agent using direct implementation.

    Args:
        fusionbase_client: Client for accessing Fusionbase data
        planner_model: LLM model instance for planning (e.g., ChatOpenAI, ChatAnthropic)
        researcher_model: LLM model instance for research (e.g., ChatOpenAI, ChatAnthropic)
        synthesizer_model: LLM model instance for synthesis (e.g., ChatOpenAI, ChatAnthropic)
        hallucination_grader_model: LLM model instance for hallucination grading (e.g., ChatOpenAI, ChatAnthropic)
        serp_api_key: API key for SERP web search
        max_iterations: Maximum iterations before forcing completion
        verbose: Enable verbose output
        proxies: Optional dictionary of proxies to use (e.g., {"http": "http://proxy:8080", "https": "https://proxy:8080"})
        verify_ssl: Whether to verify SSL certificates (set to False when using certain proxies)
    """

    if not fusionbase_client:
        raise ValueError("A valid Fusionbase client is required")

    # Validate that model instances are provided
    if not all([planner_model, researcher_model, synthesizer_model, hallucination_grader_model]):
        raise ValueError("All model instances (planner_model, researcher_model, synthesizer_model, hallucination_grader_model) are required")

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
        # Organization tools
        organization_search,
        organization_detail,

        # Relation tools
        relation_search,
        relation_detail,
        relation_resolve,

        # Web tools
        google_search,
        web_content,

        # Output tools
        GlobalFinding,
        Queries
    ]

    synthesizer_tools = [
        Introduction,
        Conclusion,
        FinalAnswer  # Keep this but make it more flexible
    ]

    # Create tool maps - fix variable name for consistency
    planner_tool_map = {tool.name: tool for tool in planner_tools}
    researcher_tool_map = {tool.name: tool for tool in researcher_tools}  # Changed from research_tool_map to researcher_tool_map
    synthesizer_tool_map = {tool.name: tool for tool in synthesizer_tools}

    # Bind tools to the provided model instances
    planner_model_with_tools = planner_model.bind_tools(planner_tools)
    researcher_model_with_tools = researcher_model.bind_tools(researcher_tools)
    synthesizer_model_with_tools = synthesizer_model.bind_tools(synthesizer_tools)
    # Hallucination grader doesn't need tools bound

    async def grade_for_hallucination(claim: str, source_content: str, context: str = "") -> Dict[str, Any]:
        """Grade whether a claim is grounded in the provided source content."""
        if verbose:
            print(f"    🔍 Grading claim for hallucination...")

        hallucination_prompt = HALLUCINATION_GRADING_INSTRUCTIONS.format(
            claim=claim,
            source_content=source_content,
            context=context
        )

        try:
            response = await hallucination_grader_model.ainvoke([
                SystemMessage(content="You are a strict fact-checking expert. Always respond with valid JSON only."),
                HumanMessage(content=hallucination_prompt)
            ])

            # Parse the JSON response
            import json
            result = json.loads(response.content)

            if verbose:
                grounded = "✅ GROUNDED" if result.get("is_grounded", False) else "❌ HALLUCINATED"
                confidence = result.get("confidence", 0.0)
                print(f"      {grounded} (confidence: {confidence:.2f})")
                if not result.get("is_grounded", False):
                    print(f"      Reason: {result.get('explanation', 'No explanation')}")

            return result

        except Exception as e:
            if verbose:
                print(f"      ⚠️ Error in hallucination grading: {str(e)}")
            # Default to not grounded if grading fails
            return {
                "is_grounded": False,
                "confidence": 0.0,
                "explanation": f"Grading failed: {str(e)}",
                "supported_parts": [],
                "unsupported_parts": [claim]
            }

    async def create_research_plan(original_query: str, target_entity: str) -> ResearchContext:
        """Create a structured research plan and context for the query."""
        if verbose:
            print(f"\n📋 Creating research plan for: '{original_query}'")

        # Determine research goal from query
        research_goal = original_query
        if "research" in original_query.lower():
            research_goal = f"Comprehensive research on {target_entity}"
        elif "find" in original_query.lower() or "what is" in original_query.lower():
            research_goal = f"Find specific information: {original_query}"

        messages = [
            SystemMessage(content=(
                f"You are a strategic research planner. Create a detailed research plan for: '{original_query}'\n\n"
                f"Target Entity: {target_entity}\n"
                f"Research Goal: {research_goal}\n\n"
                "Focus on creating a comprehensive plan that will directly answer the query. "
                "Avoid generic research areas - be specific to what the user is asking for. "
                "Examples of good key questions: 'What is the company's main business model?', 'Who are the key competitors?', 'What is the company's market position?' "
                "Examples of good data points: 'LinkedIn URL', 'Website URL', 'Number of employees', 'Revenue 2023', 'CEO name', 'Founding date' "
                "Examples of focused search strategies: 'Multi-layered keyword searches with company name variations', 'Deep social media profiling', "
                "'Industry report mining', 'News archive searches', 'Patent database searches', 'Financial filing searches', 'Executive background searches'"
            )),
            HumanMessage(content=f"Create a comprehensive research plan for: {original_query}")
        ]

        research_plan = None
        iterations = 0

        while iterations < 3 and not research_plan:
            iterations += 1

            if verbose:
                print(f"  ↪ Planning iteration {iterations}/3")

            try:
                response = await planner_model_with_tools.ainvoke(messages)
                messages.append(response)

                if hasattr(response, "tool_calls") and response.tool_calls:
                    for tool_call in response.tool_calls:
                        tool_name = tool_call["name"]
                        tool_call_id = tool_call["id"]
                        tool_args = dict(tool_call["args"])

                        if verbose:
                            arg_str = ", ".join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in tool_args.items()])
                            print(f"  🔧 Planner tool: {tool_name}({arg_str})")

                        try:
                            if tool_name == "Plan":
                                research_plan = ResearchPlan(
                                    key_questions=tool_args.get("key_questions", []),
                                    data_points=tool_args.get("data_points", []),
                                    search_strategies=tool_args.get("search_strategies", [])
                                )

                                if verbose:
                                    print("  ✅ Research plan created")
                                    print(f"    ❓ Key questions: {len(research_plan.key_questions)}")
                                    print(f"    📊 Data points: {len(research_plan.data_points)}")
                                    print(f"    🔍 Search strategies: {len(research_plan.search_strategies)}")
                            else:
                                result = f"{tool_name} skipped during planning"
                        except Exception as e:
                            result = f"Error executing {tool_name}: {str(e)}"
                            if verbose:
                                print(f"    ❌ Error: {str(e)}")

                        messages.append(
                            ToolMessage(content="Plan received", name=tool_name, tool_call_id=tool_call_id)
                        )

                    if research_plan:
                        break

                if not research_plan:
                    messages.append(
                        HumanMessage(
                            content="Please use the Plan tool to create a structured research plan."
                        )
                    )
            except Exception as e:
                if verbose:
                    print(f"  ❌ Error in planning: {str(e)}")

        # Create fallback plan if needed
        if not research_plan:
            if verbose:
                print("  ⚠️ Using fallback research plan")

            research_plan = ResearchPlan(
                key_questions=[f"What are the key facts about {target_entity}?"],
                data_points=[f"Basic information about {target_entity}"],
                search_strategies=["Fusionbase database search", "Deep web search with multiple keyword variations"]
            )

        # Create research context
        context = ResearchContext(
            original_query=original_query,
            research_goal=research_goal,
            target_entity=target_entity,
            research_plan=research_plan,
            findings_registry={},
            completed_searches=[],
            key_insights=[]
        )

        return context

    async def research_area_with_context(context: ResearchContext, area_description: str, force_org_search: bool = False) -> Dict[str, Any]:
        """Research a specific area with full context awareness and deep investigation capabilities."""
        if verbose:
            print(f"\n📝 [RESEARCH] Investigating: '{area_description[:50]}...'")

        # Build context-aware prompt with emphasis on persistence and depth
        context_info = f"""
RESEARCH CONTEXT:
- Original Query: {context.original_query}
- Research Goal: {context.research_goal}
- Target Entity: {context.target_entity}

CURRENT TASK: {area_description}

PREVIOUSLY COMPLETED SEARCHES: {', '.join(context.completed_searches) if context.completed_searches else 'None'}

EXISTING FINDINGS:
{chr(10).join([f"- {name}: {content[:100]}..." for name, content in context.findings_registry.items()]) if context.findings_registry else 'None yet'}

KEY INSIGHTS SO FAR:
{chr(10).join([f"- {insight}" for insight in context.key_insights]) if context.key_insights else 'None yet'}

YOUR MISSION: Focus specifically on finding information for "{area_description}" that directly serves the research goal: "{context.research_goal}".

PERSISTENCE AND DEPTH STRATEGY:
- If initial searches don't provide complete information, try alternative approaches
- Use Google search pagination (page parameter) to explore more results if needed
- When you find promising web pages, thoroughly extract content using web_content
- If a page doesn't have the specific information, try related searches or navigate to related pages
- Build upon previous tool results to guide your next searches
- Don't give up after one search - iterate and refine your approach
- Use the information from previous tool calls to inform your next moves

SEARCH DEPTH TECHNIQUES:
1. Start with broad searches, then narrow down based on results
2. Use company name variations and synonyms
3. Try industry-specific searches if company searches don't work
4. Paginate through Google results using the 'page' parameter (1, 2, 3, etc.)
5. Extract content from multiple promising URLs
6. Cross-reference information between different sources

You have up to {max_iterations} iterations to find comprehensive information. Use them strategically.
"""

        messages = [
            SystemMessage(content=RESEARCH_INSTRUCTIONS.format(section_description=area_description)),
            HumanMessage(content=context_info)
        ]

        # Research variables
        findings = {}
        iterations = 0
        last_response = ""
        org_search_called = False
        tool_results = []
        search_page_tracking = {}  # Track which searches and pages we've explored

        # Create models
        regular_model = researcher_model_with_tools
        forced_org_search_model = researcher_model.bind_tools(
            researcher_tools,
            tool_choice="organization_search"
        ) if force_org_search else None

        while iterations < max_iterations:
            iterations += 1

            if verbose:
                print(f"  ↪ Research iteration {iterations}/{max_iterations}")

            try:
                current_model = forced_org_search_model if (force_org_search and iterations == 1 and not org_search_called) else regular_model

                if current_model == forced_org_search_model and verbose:
                    print("    🔒 Forcing organization search in this iteration")

                response = await current_model.ainvoke(messages)
                messages.append(response)
                last_response = response.content if hasattr(response, "content") else ""

                if hasattr(response, "tool_calls") and response.tool_calls:
                    iteration_had_findings = False

                    for tool_call in response.tool_calls:
                        tool_name = tool_call["name"]
                        tool_call_id = tool_call["id"]
                        tool_args = dict(tool_call["args"])

                        if tool_name == "organization_search":
                            org_search_called = True

                        if verbose:
                            arg_str = ", ".join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}"
                                                for k, v in tool_args.items() if k not in ("client", "api_key", "proxies", "verify_ssl")])
                            print(f"  🔧 Tool: {tool_name}({arg_str})")

                        try:
                            # Add appropriate credentials and proxies
                            if tool_name in ("organization_search", "organization_detail",
                                             "relation_search", "relation_detail", "relation_resolve"):
                                tool_args["client"] = fb_client
                            elif tool_name == "google_search":
                                tool_args["api_key"] = serp_api_key
                                if proxies:
                                    tool_args["proxies"] = proxies
                                tool_args["verify_ssl"] = verify_ssl

                                # Enhanced search tracking with pagination support
                                search_query = tool_args.get("query", "")
                                search_page = tool_args.get("page", 1)  # Default to page 1
                                search_key = f"{search_query}:page{search_page}"

                                # Track this specific search + page combination
                                if search_key not in context.completed_searches:
                                    context.completed_searches.append(search_key)

                                # Update page tracking
                                if search_query not in search_page_tracking:
                                    search_page_tracking[search_query] = []
                                if search_page not in search_page_tracking[search_query]:
                                    search_page_tracking[search_query].append(search_page)

                                if verbose:
                                    print(f"    📄 Search page {search_page} for query: '{search_query}'")

                            elif tool_name == "web_content" and proxies:
                                tool_args["proxies"] = proxies
                                tool_args["verify_ssl"] = verify_ssl

                            # Execute tool
                            tool = researcher_tool_map[tool_name]
                            result = await tool.ainvoke(tool_args) if hasattr(tool, "ainvoke") else tool.invoke(tool_args)

                            tool_results.append({
                                "tool_name": tool_name,
                                "args": tool_args,
                                "result": result,
                                "iteration": iterations
                            })

                            # Handle GlobalFinding results
                            if tool_name == "GlobalFinding":
                                section_name = tool_args.get("section_name", "Untitled")
                                content = tool_args.get("content", "")
                                relevance_score = tool_args.get("relevance_score", 0.5)
                                sources = tool_args.get("sources", [])

                                # Grade for hallucination
                                source_content = "\n\n".join([
                                    f"Tool: {tr['tool_name']}\nArgs: {tr['args']}\nResult: {str(tr['result'])}"
                                    for tr in tool_results[-5:]  # Use last 5 tool results for context
                                ])

                                grading_result = await grade_for_hallucination(
                                    claim=content,
                                    source_content=source_content,
                                    context=f"Research area: {area_description}"
                                )

                                # Only add if grounded and relevant
                                if grading_result.get("is_grounded", False) and relevance_score > 0.3:
                                    # Store in global registry
                                    context.findings_registry[section_name] = content
                                    findings[section_name] = content
                                    iteration_had_findings = True

                                    # If high relevance, add to key insights
                                    if relevance_score > 0.7:
                                        insight = f"{section_name}: {content[:100]}..."
                                        if insight not in context.key_insights:
                                            context.key_insights.append(insight)

                                    if verbose:
                                        print(f"    ✅ Saved finding: '{section_name}' (relevance: {relevance_score:.2f})")
                                else:
                                    if verbose:
                                        reason = "low relevance" if relevance_score <= 0.3 else "hallucinated"
                                        print(f"    ❌ Rejected finding: '{section_name}' ({reason})")

                            # Provide rich context back to the model about what happened
                            tool_response = _create_contextual_tool_response(tool_name, result, tool_results, search_page_tracking, iterations)

                            messages.append(ToolMessage(
                                content=tool_response,
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

                    # Add guidance for next iteration if we haven't found enough yet
                    if not iteration_had_findings and iterations < max_iterations:
                        guidance = _create_iteration_guidance(tool_results, search_page_tracking, area_description, iterations)
                        messages.append(HumanMessage(content=guidance))

                        if verbose:
                            print(f"    💡 Providing guidance for iteration {iterations + 1}")

                else:
                    # No tool calls - check if we should continue or wrap up
                    if iterations < max_iterations and not findings:
                        messages.append(HumanMessage(content=(
                            f"You still have {max_iterations - iterations} iterations remaining. "
                            f"Continue searching for information about '{area_description}'. "
                            "Try different search strategies, visit web pages, or explore additional sources. "
                            "Use the tools available to find relevant information."
                        )))
                        if verbose:
                            print("  ↪ No tools called - encouraging continued search")
                    else:
                        if verbose:
                            print("  ✓ Research area complete")
                        break

            except Exception as e:
                if verbose:
                    print(f"  ❌ Research error: {str(e)}")
                break

        # Handle last response if no findings were created
        if not findings and last_response:
            source_content = "\n\n".join([
                f"Tool: {tr['tool_name']}\nResult: {str(tr['result'])}"
                for tr in tool_results
            ])

            if source_content:
                grading_result = await grade_for_hallucination(
                    claim=last_response,
                    source_content=source_content,
                    context=f"Research area: {area_description}"
                )

                if grading_result.get("is_grounded", False):
                    findings["Summary"] = last_response
                    context.findings_registry[f"{area_description} - Summary"] = last_response

        return {
            "area": area_description,
            "findings": findings,
            "iterations": iterations,
            "last_response": last_response,
            "org_search_called": org_search_called,
            "search_pages_explored": search_page_tracking
        }

    def _create_contextual_tool_response(tool_name: str, result: Any, tool_results: List[Dict], search_page_tracking: Dict, iteration: int) -> str:
        """Create a contextual response that helps the model understand what happened and what to do next."""

        base_result = str(result)

        # Add specific guidance based on tool type and results
        if tool_name == "google_search":
            if isinstance(result, dict) and "organic_results" in result:
                organic_count = len(result["organic_results"])
                context_info = f"\n\nCONTEXT: Found {organic_count} search results. "

                if organic_count > 0:
                    context_info += "Consider using web_content on promising URLs to extract detailed information. "
                    context_info += "You can also try different search terms or use the 'page' parameter (2, 3, etc.) to see more results."
                else:
                    context_info += "No results found. Try different search terms or keywords."

                return base_result + context_info

        elif tool_name == "web_content":
            if isinstance(result, dict) and result.get("content"):
                content_length = len(result["content"])
                context_info = f"\n\nCONTEXT: Extracted {content_length} characters of content. "

                if content_length > 100:
                    context_info += "Review this content for relevant information. If it doesn't contain what you need, try other URLs or search strategies."
                else:
                    context_info += "Limited content extracted. Consider trying other URLs or search approaches."

                return base_result + context_info

        elif tool_name in ["organization_search", "relation_search"]:
            if isinstance(result, list) and len(result) > 0:
                context_info = f"\n\nCONTEXT: Found {len(result)} database results. Consider using detail tools to get more information about specific entities."
                return base_result + context_info

        return base_result

    def _create_iteration_guidance(tool_results: List[Dict], search_page_tracking: Dict, area_description: str, current_iteration: int) -> str:
        """Create guidance for the next iteration based on what's been tried so far."""

        # Analyze what's been done
        tools_used = [tr["tool_name"] for tr in tool_results]
        searches_done = [tr for tr in tool_results if tr["tool_name"] == "google_search"]
        web_content_extractions = [tr for tr in tool_results if tr["tool_name"] == "web_content"]

        guidance_parts = [
            f"ITERATION {current_iteration + 1} GUIDANCE:",
            f"You're researching: '{area_description}'"
        ]

        # Suggest strategies based on what's been tried
        if "google_search" not in tools_used:
            guidance_parts.append("• Consider starting with a Google search to find relevant information")
        elif len(searches_done) == 1:
            guidance_parts.append("• Try alternative search terms, or use the 'page' parameter to see more Google results (page=2, page=3, etc.)")
        elif len(web_content_extractions) == 0 and searches_done:
            guidance_parts.append("• You found search results but haven't extracted content from any pages. Use web_content on promising URLs")
        else:
            guidance_parts.append("• Try different search strategies: use synonyms, industry terms, or more specific queries")

        # Add specific suggestions based on search tracking
        if search_page_tracking:
            for query, pages in search_page_tracking.items():
                max_page = max(pages)
                if max_page == 1:
                    guidance_parts.append(f"• Consider exploring page 2+ for query '{query}' using page=2 parameter")

        guidance_parts.append("• Don't give up - each iteration brings you closer to finding the information needed")

        return "\n".join(guidance_parts)

    async def synthesize_final_report(context: ResearchContext) -> str:
        """Synthesize all findings into a comprehensive final report."""
        if verbose:
            print("\n🔄 Synthesizing final report")

        if not context.findings_registry:
            if verbose:
                print("  ⚠️ No findings to synthesize")
            return f"# Research Report: {context.target_entity}\n\nUnable to find specific information about {context.target_entity} related to your query."

        # Prepare comprehensive context for intelligent synthesis
        context_text = f"""
RESEARCH CONTEXT:
Original Query: {context.original_query}
Research Goal: {context.research_goal}
Target Entity: {context.target_entity}

RESEARCH PLAN EXECUTED:
Key Questions: {', '.join(context.research_plan.key_questions) if context.research_plan else 'None'}
Data Points: {', '.join(context.research_plan.data_points) if context.research_plan else 'None'}
Search Strategies: {', '.join(context.research_plan.search_strategies) if context.research_plan else 'None'}

KEY INSIGHTS DISCOVERED:
{chr(10).join([f"- {insight}" for insight in context.key_insights]) if context.key_insights else 'No key insights recorded'}

ALL RESEARCH FINDINGS:
{chr(10).join([f"## {name}{chr(10)}{content}{chr(10)}" for name, content in context.findings_registry.items()])}

SEARCHES COMPLETED: {', '.join(context.completed_searches) if context.completed_searches else 'None'}
"""

        messages = [
            SystemMessage(content=SYNTHESIS_INSTRUCTIONS),
            HumanMessage(content=context_text)
        ]

        try:
            response = await synthesizer_model_with_tools.ainvoke(messages)

            if hasattr(response, "tool_calls") and response.tool_calls:
                final_content = None

                for tool_call in response.tool_calls:
                    tool_name = tool_call["name"]
                    tool_args = dict(tool_call["args"])

                    if verbose:
                        format_type = tool_args.get('format_type', 'intelligent')
                        print(f"  🔧 Using: {tool_name} (format: {format_type})")

                    if tool_name == "FinalAnswer":
                        final_content = tool_args.get("content", "")

                        if verbose:
                            print(f"  ✅ Final answer synthesized intelligently")

                        return final_content

                    # Handle legacy tools for backward compatibility
                    elif tool_name in ["Introduction", "Conclusion"]:
                        tool = synthesizer_tool_map[tool_name]
                        result = await tool.ainvoke(tool_args) if hasattr(tool, "ainvoke") else tool.invoke(tool_args)

                        if not final_content:
                            # If no FinalAnswer was provided, fall back to traditional format
                            if tool_name == "Introduction":
                                intro_content = f"# {result.name}\n\n{result.content}"
                            elif tool_name == "Conclusion":
                                conclusion_content = f"## {result.name}\n\n{result.content}"

                # If we got legacy format, assemble traditional report
                if not final_content and ('intro_content' in locals() or 'conclusion_content' in locals()):
                    body_parts = []
                    for section_name, content in context.findings_registry.items():
                        body_parts.append(f"## {section_name}\n\n{content}")

                    body_content = "\n\n".join(body_parts)
                    intro = locals().get('intro_content', f"# Research Report: {context.target_entity}")
                    conclusion = locals().get('conclusion_content', "## Summary\nResearch completed.")
                    final_content = f"{intro}\n\n{body_content}\n\n{conclusion}"

            # Fallback to direct response if no tools were used
            if not final_content and response.content:
                final_content = response.content

            # Return the final content
            if final_content:
                return final_content

        except Exception as e:
            if verbose:
                print(f"  ❌ Synthesis error: {str(e)}")

        # Final fallback - create a simple markdown report
        parts = [f"# Research Report: {context.target_entity}\n"]
        parts.append(f"## Research Query\n{context.original_query}\n")

        for section_name, content in context.findings_registry.items():
            parts.append(f"## {section_name}\n{content}\n")

        parts.append(f"## Summary\nResearch completed with {len(context.findings_registry)} findings.")

        return "\n".join(parts)

    async def ainvoke(initial_state):
        """Execute the research agent process with context awareness."""

        # Extract query and determine target entity
        user_messages = initial_state.get("messages", [])
        if not user_messages:
            raise ValueError("No query provided")

        if isinstance(user_messages[0], dict):
            user_query = user_messages[0].get("content", "")
        elif hasattr(user_messages[0], "content"):
            user_query = user_messages[0].content
        else:
            raise ValueError("Invalid message format")

        if not user_query:
            raise ValueError("Empty query provided")

        # Extract target entity from query - fix the case sensitivity issue
        target_entity = initial_state.get("company_topic", "the requested topic")
        if target_entity == "the requested topic":
            # Try to extract from query with better logic
            user_query_lower = user_query.lower()
            for phrase in ["about ", "for ", "on "]:
                if phrase in user_query_lower:
                    start_pos = user_query_lower.find(phrase) + len(phrase)
                    # Look for the end of the word/phrase (space, punctuation, or end of string)
                    remaining = user_query[start_pos:]
                    end_pos = len(remaining)
                    for delimiter in [" ", "?", ".", ",", "!", "\n"]:
                        delim_pos = remaining.find(delimiter)
                        if delim_pos != -1 and delim_pos < end_pos:
                            end_pos = delim_pos

                    if end_pos > 0:
                        target_entity = remaining[:end_pos].strip()
                        break

            # If still not found, try to extract company names (simple heuristic)
            if target_entity == "the requested topic":
                # Look for capitalized words that might be company names
                import re
                words = user_query.split()
                for i, word in enumerate(words):
                    if word[0].isupper() and len(word) > 2:
                        # Check if next word is also capitalized (compound company name)
                        if i + 1 < len(words) and words[i + 1][0].isupper():
                            target_entity = f"{word} {words[i + 1]}"
                        else:
                            target_entity = word
                        break

        if verbose:
            print(f"\n🚀 Starting context-aware research")
            print(f"🎯 Target Entity: {target_entity}")
            print(f"🔎 Query: '{user_query}'")

        try:
            # PHASE 1: Create research context and plan
            research_context = await create_research_plan(user_query, target_entity)

            # PHASE 2: Execute research areas with context
            if verbose:
                print(f"\n🔍 Executing research with {len(research_context.research_plan.key_questions)} questions and {len(research_context.research_plan.data_points)} data points")

            research_areas = []
            for question in research_context.research_plan.key_questions:
                research_areas.append(f"Question: {question}")
            for data_point in research_context.research_plan.data_points:
                research_areas.append(f"Data Point: {data_point}")

            if not research_areas:
                research_areas = ["Basic company information"]

            # Execute research with context awareness
            research_tasks = []
            for i, area in enumerate(research_areas):
                force_org_search = (i == 0)
                research_tasks.append(research_area_with_context(research_context, area, force_org_search))

            await asyncio.gather(*research_tasks)

            # PHASE 3: Generate final report
            final_report = await synthesize_final_report(research_context)

            if verbose:
                print(f"\n🏁 Research complete")
                print(f"📊 Total findings: {len(research_context.findings_registry)}")
                print(f"💡 Key insights: {len(research_context.key_insights)}")
                print(f"🔍 Searches completed: {len(research_context.completed_searches)}")

            return {"final_report": final_report}

        except Exception as e:
            if verbose:
                print(f"\n❌ Error in research: {str(e)}")
                import traceback
                traceback.print_exc()

            return {
                "final_report": f"# Research Error\n\nAn error occurred while researching {target_entity}: {str(e)}"
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
