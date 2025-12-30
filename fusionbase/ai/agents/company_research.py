"""Company research agent using LangChain, OpenAI, and Fusionbase tools."""

import asyncio
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple, TypedDict, Union
from urllib.parse import urlparse

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
from fusionbase.ai.tools.entity.person import person_detail
from fusionbase.ai.tools.entity.person import person_search
from fusionbase.ai.tools.entity.relation import relation_detail
from fusionbase.ai.tools.entity.relation import relation_resolve
from fusionbase.ai.tools.entity.relation import relation_search
from fusionbase.ai.tools.relation import annual_financial_statements
from fusionbase.ai.tools.relation import balance_sheet_accounts
from fusionbase.ai.tools.relation import financial_kpi
from fusionbase.ai.tools.relation import insolvency_publications
from fusionbase.ai.tools.relation import network
from fusionbase.ai.tools.relation import news
from fusionbase.ai.tools.relation import profit_and_loss_account
from fusionbase.ai.tools.relation import publications
from fusionbase.ai.tools.relation import related_persons
from fusionbase.ai.tools.web.content import web_content
from fusionbase.ai.tools.web.search import google_search

from .prompts import HALLUCINATION_GRADING_INSTRUCTIONS
from .prompts import RESEARCH_INSTRUCTIONS
from .prompts import SYNTHESIS_INSTRUCTIONS
from .token_efficient_tools_v2 import IntentBasedTokenHandler

# pylint: disable=line-too-long,too-many-arguments,too-many-locals,too-many-branches,too-many-statements
# pylint: disable=too-many-nested-blocks,unused-variable,redefined-outer-name,too-many-positional-arguments


@tool
class Section(BaseModel):
    """A section of the company research report."""
    name: str = Field(description="Name for this section of the report.",)
    content: str = Field(description="The content of the section.")


@tool
class Sections(BaseModel):
    """Define the sections of the company research report."""
    sections: List[str] = Field(
        description="Sections of the report with detailed descriptions.",)


@tool
class Introduction(BaseModel):
    """Introduction for the company research report."""
    name: str = Field(description="Name for the report.",)
    content: str = Field(
        description=
        "The content of the introduction, giving an overview of the report.")


@tool
class Conclusion(BaseModel):
    """Conclusion for the company research report."""
    name: str = Field(description="Name for the conclusion of the report.",)
    content: str = Field(
        description="The content of the conclusion, summarizing the report.")


@tool
class Queries(BaseModel):
    """Search queries for gathering information."""
    queries: List[str] = Field(
        description="List of search queries to gather information.",)


@tool
class FinalAnswer(BaseModel):
    """Final answer formatted according to user's requirements."""
    content: str = Field(
        description="The final answer formatted exactly as requested by the user"
    )
    format_type: str = Field(
        description=
        "The format type detected (e.g., 'json', 'markdown', 'list', 'plain_text')"
    )


class CompanyResearchResult(TypedDict):
    """Result of the company research agent."""
    final_report: Union[str, Dict[str, Any]]


class ResearchContext(BaseModel):
    """Global research context shared across all agents."""
    original_query: str
    research_goal: str
    target_entity: str
    research_plan: Optional["ResearchPlan"] = None
    findings_registry: Dict[str, str] = {}  # section_name -> content
    completed_searches: List[str] = []  # Track what's been searched
    key_insights: List[str] = []  # Store important discoveries

    # NEW: Global Fusionbase entity data
    fusionbase_entity: Optional[Dict[
        str, Any]] = None  # The found organization entity
    fusionbase_entity_id: Optional[str] = None  # The entity ID
    fusionbase_search_completed: bool = False  # Whether we've done the search
    fusionbase_detail_completed: bool = False  # Whether we've got the details


class ResearchPlan(BaseModel):
    """A plan for researching the topic."""
    key_questions: List[str]  # Questions that need to be answered
    data_points: List[str]  # Specific data points to gather
    search_strategies: List[str]  # Strategies for finding information


@tool
class Plan(BaseModel):
    """Define a research plan for the query."""
    key_questions: List[str] = Field(
        description=
        "Key questions that need to be answered to fulfill the research goal (e.g., 'What is the company's main business?', 'Who are the key executives?')"
    )
    data_points: List[str] = Field(
        description=
        "Specific data points to gather about the company (e.g., 'LinkedIn URL', 'Website URL', 'Number of employees', 'Founding date', 'CEO name')"
    )
    search_strategies: List[str] = Field(
        description=
        "Deep search strategies to use for comprehensive information gathering (e.g., 'Deep web search with multiple keyword variations', 'Social media profile searches', 'Industry database lookups', 'News article mining', 'Company filing searches')"
    )


@tool
class GlobalFinding(BaseModel):
    """Store a research finding in the global registry."""
    section_name: str = Field(
        description=
        "Name/category of this finding (e.g., 'Company Overview', 'LinkedIn URL', 'Financial Data')"
    )
    content: str = Field(description="The research finding content")
    relevance_score: float = Field(
        description=
        "How relevant this finding is to the research goal (0.0-1.0)",
        ge=0.0,
        le=1.0)
    sources: List[str] = Field(
        description="Sources where this information was found", default=[])


@tool
class ResearchReflection(BaseModel):
    """Assess whether research findings provide sufficient information for the area."""
    is_sufficient: bool = Field(
        description=
        "Whether the research findings are sufficient to answer the research question"
    )
    confidence: float = Field(
        description="Confidence level in the sufficiency assessment (0.0-1.0)",
        ge=0.0,
        le=1.0)
    reasoning: str = Field(
        description=
        "Brief explanation of why the research is sufficient or insufficient")
    missing_information: List[str] = Field(
        description=
        "List of specific information that is still needed (if insufficient)",
        default=[])


@tool
class ExtractCompanyName(BaseModel):
    """Extract the actual company name from a research query."""
    company_name: str = Field(
        description=
        "The clean company name extracted from the query (e.g., 'Apple Inc', 'Microsoft Corporation', 'BMW AG')"
    )


def _parse_proxy_config(
    proxies: Union[Dict[str, str], Dict[str, Dict[str, str]], None]
) -> Dict[str, Dict[str, str]]:
    """Parse proxy configuration to support both simple and domain-specific formats.

    Args:
        proxies: Proxy configuration in one of these formats:
            - Simple: {"http": "proxy_url", "https": "proxy_url"}
            - Domain-specific: {
                "*": {"http": "default_proxy", "https": "default_proxy"},
                "linkedin.com": {"http": "linkedin_proxy", "https": "linkedin_proxy"},
                "*.linkedin.com": {"http": "linkedin_proxy", "https": "linkedin_proxy"}
            }

    Returns:
        Normalized domain-specific proxy configuration
    """
    if not proxies:
        return {}

    # Check if this is a simple proxy config (has http/https keys directly)
    if any(key in proxies for key in ["http", "https"]):
        # Convert simple format to domain-specific format with wildcard
        return {"*": proxies}

    # Already in domain-specific format
    return proxies


def _get_proxy_for_url(
        url: str, proxy_config: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """Get the appropriate proxy configuration for a given URL.

    Args:
        url: The URL to get proxy for
        proxy_config: Domain-specific proxy configuration

    Returns:
        Proxy configuration dict for the URL's domain
    """
    if not proxy_config:
        return {}

    parsed_url = urlparse(url)
    domain = parsed_url.netloc.lower()

    # Remove port if present
    if ":" in domain:
        domain = domain.split(":")[0]

    # Check for exact domain match first
    if domain in proxy_config:
        return proxy_config[domain]

    # Check for pattern matches (including wildcards)
    for pattern, proxy_settings in proxy_config.items():
        if _domain_matches_pattern(domain, pattern):
            return proxy_settings

    # Fall back to wildcard if available
    if "*" in proxy_config:
        return proxy_config["*"]

    return {}


def _domain_matches_pattern(domain: str, pattern: str) -> bool:
    """Check if a domain matches a pattern (supporting wildcards).

    Args:
        domain: The domain to check (e.g., "de.linkedin.com")
        pattern: The pattern to match against (e.g., "*.linkedin.com", "linkedin.com")

    Returns:
        True if domain matches pattern
    """
    if pattern == "*":
        return True

    if pattern == domain:
        return True

    # Handle wildcard patterns
    if "*" in pattern:
        # Convert pattern to regex
        # Escape special regex characters except *
        escaped_pattern = re.escape(pattern).replace(r"\*", ".*")
        regex_pattern = f"^{escaped_pattern}$"
        return bool(re.match(regex_pattern, domain))

    return False


def create_company_research_agent(
        fusionbase_client: Fusionbase,
        planner_model: Any,
        researcher_model: Any,
        synthesizer_model: Any,
        hallucination_grader_model: Any,
        serp_api_key: str = None,
        max_iterations: int = 10,
        verbose: bool = False,
        proxies: Optional[Union[Dict[str, str], Dict[str, Dict[str,
                                                               str]]]] = None,
        verify_ssl: bool = True,
        planner_tools: Optional[List[Any]] = None,
        researcher_tools: Optional[List[Any]] = None,
        synthesizer_tools: Optional[List[Any]] = None,
        output_schema: Optional[Union[Dict[str, Any], type[BaseModel]]] = None):
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
        proxies: Proxy configuration. Can be either:
            - Simple: {"http": "http://proxy:8080", "https": "https://proxy:8080"}
            - Domain-specific: {
                "*": {"http": "http://default-proxy:8080", "https": "https://default-proxy:8080"},
                "linkedin.com": {"http": "http://linkedin-proxy:8080", "https": "https://linkedin-proxy:8080"},
                "*.linkedin.com": {"http": "http://linkedin-proxy:8080", "https": "https://linkedin-proxy:8080"}
            }
        verify_ssl: Whether to verify SSL certificates (set to False when using certain proxies)
        planner_tools: Optional custom list of tools for the planner. If not provided, uses default planner tools.
        researcher_tools: Optional custom list of tools for the researcher. If not provided, uses default researcher tools
            including organization tools, relation tools, web tools, and output tools.
        synthesizer_tools: Optional custom list of tools for the synthesizer. If not provided, uses default synthesizer tools.
        output_schema: Optional schema for structured output. Can be either:
            - A Pydantic model class for typed structured output
            - A dictionary/JSON schema for flexible structured output
            When provided, the synthesizer will return data conforming to this schema instead of markdown.
    """

    if not fusionbase_client:
        raise ValueError("A valid Fusionbase client is required")

    # Validate that model instances are provided
    if not all([
            planner_model, researcher_model, synthesizer_model,
            hallucination_grader_model
    ]):
        raise ValueError(
            "All model instances (planner_model, researcher_model, synthesizer_model, hallucination_grader_model) are required"
        )

    # Initialize the client
    fb_client = fusionbase_client

    # Get SERP API key
    if not serp_api_key:
        serp_api_key = os.environ.get("SERP_API_KEY")

    # Parse proxy configuration for domain-specific routing
    proxy_config = _parse_proxy_config(proxies)
    if verbose and proxy_config:
        print("🌐 Proxy configuration:")
        for domain_pattern, proxy_settings in proxy_config.items():
            http_proxy = proxy_settings.get("http", "None")
            https_proxy = proxy_settings.get("https", "None")
            print(f"  {domain_pattern}: HTTP={http_proxy}, HTTPS={https_proxy}")

    # Define tools for different agent roles
    # Use custom tools if provided, otherwise use defaults
    if planner_tools is None:
        planner_tools = [
            organization_search,
            google_search,
            Plan,
            ExtractCompanyName  # Add the extraction tool to planner
        ]

    # Define internal tools that are always required
    researcher_internal_tools = [GlobalFinding, ResearchReflection, Queries]

    if researcher_tools is None:
        # Default external tools
        researcher_tools = [
            # Organization tools
            organization_search,
            organization_detail,

            # Person tools
            person_search,
            person_detail,

            # Relation tools
            relation_search,
            relation_detail,
            relation_resolve,

            # Convenience relation tools
            financial_kpi,
            network,
            related_persons,
            profit_and_loss_account,
            publications,
            balance_sheet_accounts,
            insolvency_publications,
            annual_financial_statements,
            news,

            # Web tools
            google_search,
            web_content,
        ]

    # Always append internal tools to whatever tools were provided
    researcher_tools = researcher_tools + researcher_internal_tools

    if synthesizer_tools is None:
        synthesizer_tools = [
            Introduction,
            Conclusion,
            FinalAnswer  # Keep this but make it more flexible
        ]

    # Create tool maps - fix variable name for consistency
    planner_tool_map = {tool.name: tool for tool in planner_tools}
    researcher_tool_map = {
        tool.name: tool for tool in researcher_tools
    }  # Changed from research_tool_map to researcher_tool_map
    synthesizer_tool_map = {tool.name: tool for tool in synthesizer_tools}

    # Bind tools to the provided model instances
    planner_model_with_tools = planner_model.bind_tools(planner_tools)
    researcher_model_with_tools = researcher_model.bind_tools(researcher_tools)
    synthesizer_model_with_tools = synthesizer_model.bind_tools(
        synthesizer_tools)
    # Hallucination grader doesn't need tools bound

    # Track tool calls to prevent duplicates within a single run
    seen_tool_calls: Set[Tuple[str, str]] = set()
    tool_call_results: Dict[Tuple[str, str], Any] = {}

    # Track early termination statistics
    early_termination_stats = {
        "total_research_areas": 0,
        "early_terminations": 0,
        "termination_reasons": []
    }

    # Initialize intent-based token handler
    token_handler = IntentBasedTokenHandler()

    async def execute_tool(tool, tool_name: str, tool_args: Dict[str, Any]):
        """Execute a tool with simple deduplication that prevents unnecessary API calls."""
        # Remove non-essential args for deduplication (these don't affect the core operation)
        filtered_args = {
            k: v
            for k, v in tool_args.items()
            if k not in ("client", "api_key", "proxies", "verify_ssl")
        }

        # Create a cache key from tool name and filtered arguments
        try:
            # Use a more stable serialization approach
            args_str = f"{tool_name}:" + "|".join(
                f"{k}={v}" for k, v in sorted(filtered_args.items()))
        except Exception:
            args_str = f"{tool_name}:{str(filtered_args)}"

        # Check if we've already executed this exact tool call
        if args_str in seen_tool_calls:
            if verbose:
                print(
                    f"    🔄 Using cached result: {tool_name}({', '.join([f'{k}={v}' for k, v in filtered_args.items()])})"
                )

            # Return the cached result if available
            cached_result = tool_call_results.get(args_str)
            if cached_result is not None:
                return cached_result
            else:
                # If no cached result, remove from seen_tool_calls and execute
                if verbose:
                    print(
                        f"    ⚠️ Cache inconsistency detected, executing tool")
                seen_tool_calls.discard(args_str)

        # Execute the tool
        if verbose:
            print(
                f"    🔧 Executing: {tool_name}({', '.join([f'{k}={v}' for k, v in filtered_args.items()])})"
            )

        try:
            if hasattr(tool, "ainvoke"):
                result = await tool.ainvoke(tool_args)
            else:
                result = tool.invoke(tool_args)

        except Exception as e:
            if verbose:
                print(f"    ❌ Tool execution failed: {str(e)}")
            result = [{"error": f"Tool execution failed: {str(e)}"}]

        # Cache the result AFTER successful execution
        seen_tool_calls.add(args_str)
        tool_call_results[args_str] = result

        # No verbose logging of results - keep output clean
        if verbose:
            print(f"    ✅ Completed: {tool_name}")

        return result

    async def grade_for_hallucination(claim: str,
                                      source_content: str,
                                      context: str = "") -> Dict[str, Any]:
        """Grade whether a claim is grounded in the provided source content."""
        if verbose:
            print(f"    🔍 Grading claim for hallucination...")

        # Truncate source content to reduce tokens
        truncated_source = source_content
        if len(source_content) > 8000:
            truncated_source = source_content[:8000] + "... [content truncated]"
            if verbose:
                print(
                    f"    📄 Source content truncated from {len(source_content)} to 8000 characters"
                )

        hallucination_prompt = HALLUCINATION_GRADING_INSTRUCTIONS.format(
            claim=claim, source_content=truncated_source, context=context)

        try:
            response = await hallucination_grader_model.ainvoke([
                SystemMessage(
                    content=
                    "You are a strict fact-checking expert. Always respond with valid JSON only. Do NOT use markdown formatting or code blocks."
                ),
                HumanMessage(content=hallucination_prompt)
            ])

            # Clean the response content from markdown formatting
            content = response.content.strip()

            # Remove markdown code blocks if present
            if content.startswith("```json"):
                content = content[7:]  # Remove ```json
            elif content.startswith("```"):
                content = content[3:]  # Remove ```

            if content.endswith("```"):
                content = content[:-3]  # Remove trailing ```

            content = content.strip()

            # Parse the JSON response
            try:
                result = json.loads(content)
            except json.JSONDecodeError as json_error:
                # Only show raw response when JSON parsing fails
                if verbose:
                    print(f"    ❌ JSON decode error: {json_error}")
                    print(
                        f"    📄 Raw grading response: {response.content[:200]}..."
                    )
                    print(f"    📄 Cleaned response content: {content}")

                # Try to extract JSON from response if it's still wrapped in other text
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    try:
                        result = json.loads(json_match.group())
                        if verbose:
                            print(
                                f"    ✅ Successfully extracted JSON from wrapped response"
                            )
                    except json.JSONDecodeError:
                        if verbose:
                            print(f"    ❌ Failed to parse extracted JSON")
                        raise json_error
                else:
                    if verbose:
                        print(f"    ❌ No JSON found in response")
                    raise json_error

            if verbose:
                grounded = "✅ GROUNDED" if result.get(
                    "is_grounded", False) else "❌ HALLUCINATED"
                confidence = result.get("confidence", 0.0)
                print(f"      {grounded} (confidence: {confidence:.2f})")
                if not result.get("is_grounded", False):
                    print(
                        f"      Reason: {result.get('explanation', 'No explanation')}"
                    )

            return result

        except Exception as e:
            if verbose:
                print(f"      ⚠️ Error in hallucination grading: {str(e)}")
                print(f"      📄 Error type: {type(e).__name__}")
                if hasattr(e, 'response'):
                    print(f"      📄 Response object: {e.response}")

            # Default to not grounded if grading fails
            return {
                "is_grounded": False,
                "confidence": 0.0,
                "explanation": f"Grading failed: {str(e)}",
                "supported_parts": [],
                "unsupported_parts": [claim]
            }

    async def create_research_plan(
            original_query: str,
            target_entity: str,
            system_content: Optional[str] = None) -> ResearchContext:
        """Create a structured research plan and context for the query."""
        if verbose:
            print(f"\n📋 Creating research plan for: '{original_query}'")

        # Determine research goal from query
        research_goal = original_query
        if "research" in original_query.lower():
            research_goal = f"Comprehensive research on {target_entity}"
        elif "find" in original_query.lower(
        ) or "what is" in original_query.lower():
            research_goal = f"Find specific information: {original_query}"

        # Base system prompt for planning
        base_system_content = (
            f"You are a strategic research planner. Create a detailed research plan for: '{original_query}'\n\n"
            f"Target Entity: {target_entity}\n"
            f"Research Goal: {research_goal}\n\n"
            "Focus on creating a comprehensive plan that will directly answer the query. "
            "Avoid generic research areas - be specific to what the user is asking for. "
            "Examples of good key questions: 'What is the company's main business model?', 'Who are the key competitors?', 'What is the company's market position?' "
            "Examples of good data points: 'LinkedIn URL', 'Website URL', 'Number of employees', 'Revenue 2023', 'CEO name', 'Founding date' "
            "Examples of focused search strategies: 'Multi-layered keyword searches with company name variations', 'Deep social media profiling', "
            "'Industry report mining', 'News archive searches', 'Patent database searches', 'Financial filing searches', 'Executive background searches'"
        )

        # If we have a schema, add field hints to the planning prompt
        if output_schema:
            schema_hint = "\n\n🎯 STRUCTURED OUTPUT REQUIREMENTS:"
            if isinstance(output_schema, dict):
                # JSON schema - extract field names and descriptions
                properties = output_schema.get("properties", {})
                required_fields = output_schema.get("required", [])
                field_descriptions = []
                for field, spec in properties.items():
                    desc = spec.get("description", field)
                    req_marker = " (REQUIRED)" if field in required_fields else ""
                    field_descriptions.append(
                        f"  • {field}: {desc}{req_marker}")
                schema_hint += "\nThe research must gather information for these specific fields:\n" + "\n".join(
                    field_descriptions)
                schema_hint += "\n\nFocus your research plan specifically on finding data for these fields."
            elif hasattr(output_schema, "__fields__"):
                # Pydantic model - extract field names
                field_names = list(output_schema.__fields__.keys())
                schema_hint += f"\nThe research must gather information for these specific fields: {', '.join(field_names)}"
                schema_hint += "\n\nFocus your research plan specifically on finding data for these fields."

            base_system_content += schema_hint

        # Append custom system content if provided
        full_system_content = base_system_content
        if system_content:
            full_system_content += f"\n\n{system_content}"

        # User content for planning
        user_content = f"Create a comprehensive research plan for: {original_query}"

        messages = [
            SystemMessage(content=full_system_content),
            HumanMessage(content=user_content)
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
                            arg_str = ", ".join([
                                f"{k}='{v}'"
                                if isinstance(v, str) else f"{k}={v}"
                                for k, v in tool_args.items()
                            ])
                            print(f"  🔧 Planner tool: {tool_name}({arg_str})")

                        try:
                            if tool_name == "Plan":
                                research_plan = ResearchPlan(
                                    key_questions=tool_args.get(
                                        "key_questions", []),
                                    data_points=tool_args.get(
                                        "data_points", []),
                                    search_strategies=tool_args.get(
                                        "search_strategies", []))

                                if verbose:
                                    print("  ✅ Research plan created")
                                    print(
                                        f"    ❓ Key questions: {len(research_plan.key_questions)}"
                                    )
                                    print(
                                        f"    📊 Data points: {len(research_plan.data_points)}"
                                    )
                                    print(
                                        f"    🔍 Search strategies: {len(research_plan.search_strategies)}"
                                    )
                            else:
                                result = f"{tool_name} skipped during planning"
                        except Exception as e:
                            result = f"Error executing {tool_name}: {str(e)}"
                            if verbose:
                                print(f"    ❌ Error: {str(e)}")

                        messages.append(
                            ToolMessage(content="Plan received",
                                        name=tool_name,
                                        tool_call_id=tool_call_id))

                    if research_plan:
                        break

                if not research_plan:
                    messages.append(
                        HumanMessage(
                            content=
                            "Please use the Plan tool to create a structured research plan."
                        ))
            except Exception as e:
                if verbose:
                    print(f"  ❌ Error in planning: {str(e)}")

        # Create fallback plan if needed
        if not research_plan:
            if verbose:
                print("  ⚠️ Using fallback research plan")

            research_plan = ResearchPlan(
                key_questions=[
                    f"What are the key facts about {target_entity}?"
                ],
                data_points=[f"Basic information about {target_entity}"],
                search_strategies=[
                    "Fusionbase database search",
                    "Deep web search with multiple keyword variations"
                ])

        # Create research context
        context = ResearchContext(original_query=original_query,
                                  research_goal=research_goal,
                                  target_entity=target_entity,
                                  research_plan=research_plan,
                                  findings_registry={},
                                  completed_searches=[],
                                  key_insights=[])

        return context

    async def extract_clean_company_name(original_query: str) -> str:
        """Use LLM to extract clean company name from query."""
        if verbose:
            print(f"\n🏷️ Extracting company name from: '{original_query}'")

        extraction_prompt = f"""Extract the actual company name from this research query: "{original_query}"

Examples:
- "Research Apple Inc and provide company overview" → "Apple Inc"
- "What is Microsoft Corporation's LinkedIn URL?" → "Microsoft Corporation"
- "Find BMW AG contact information" → "BMW AG"
- "Tell me about Tesla Inc's website" → "Tesla Inc"
- "Get information about Fusionbase GmbH" → "Fusionbase GmbH"

Return only the clean company name, no additional text."""

        try:
            # Force the model to use the ExtractCompanyName tool
            response = await planner_model_with_tools.ainvoke(
                [HumanMessage(content=extraction_prompt)],
                tool_choice={
                    "type": "function",
                    "function": {
                        "name": "ExtractCompanyName"
                    }
                })

            if hasattr(response, "tool_calls") and response.tool_calls:
                for tool_call in response.tool_calls:
                    if tool_call["name"] == "ExtractCompanyName":
                        extracted_name = tool_call["args"].get(
                            "company_name", "").strip()
                        if extracted_name and len(extracted_name) > 1:
                            if verbose:
                                print(
                                    f"    ✅ Extracted company name: '{extracted_name}'"
                                )
                            return extracted_name

        except Exception as e:
            if verbose:
                print(f"    ❌ Extraction failed: {str(e)}")

        # Fallback to a simple extraction if LLM fails
        if verbose:
            print("    ↪ Falling back to simple extraction")

        # Simple fallback logic
        query_lower = original_query.lower()
        for phrase in ["about ", "for ", "on ", "of "]:
            if phrase in query_lower:
                start_pos = query_lower.find(phrase) + len(phrase)
                remaining = original_query[start_pos:]
                end_pos = len(remaining)
                for delimiter in [
                        " and ", " linkedin", " website", " contact", "?", ".",
                        ",", "!", "\n"
                ]:
                    delim_pos = remaining.lower().find(delimiter)
                    if delim_pos != -1 and delim_pos < end_pos:
                        end_pos = delim_pos

                if end_pos > 0:
                    extracted = remaining[:end_pos].strip()
                    if len(extracted) > 1:
                        return extracted

        return original_query  # Final fallback

    async def establish_fusionbase_foundation(context: ResearchContext) -> bool:
        """Establish the foundational Fusionbase entity data for the entire research mission."""
        if verbose:
            print(
                f"\n🏛️ Establishing Fusionbase foundation for: {context.target_entity}"
            )

        # Skip if already completed
        if context.fusionbase_search_completed and context.fusionbase_detail_completed:
            if verbose:
                print("  ✅ Fusionbase foundation already established")
            return True

        # Step 1: Organization Search (if not done)
        if not context.fusionbase_search_completed:
            if verbose:
                print("  🔍 Performing global organization search...")

            try:
                # Use LLM to extract clean company name for Fusionbase search
                company_name = await extract_clean_company_name(
                    context.original_query)
                if company_name != context.target_entity:
                    # Update target entity with cleaner name if extraction improved it
                    if len(company_name) > 2 and not any(
                            word in company_name.lower() for word in
                        ['research', 'find', 'what', 'information']):
                        context.target_entity = company_name
                        if verbose:
                            print(
                                f"    📝 Refined company name: '{company_name}'")

                search_result = await execute_tool(
                    researcher_tool_map["organization_search"],
                    "organization_search",
                    {
                        "query": context.target_entity,
                        "client": fb_client
                    }  # Use clean company name
                )

                context.fusionbase_search_completed = True

                # Check if we found an entity
                if isinstance(search_result, list) and len(search_result) > 0:
                    first_result = search_result[0]
                    if isinstance(first_result,
                                  dict) and "entity_id" in first_result:
                        context.fusionbase_entity_id = first_result["entity_id"]

                        # Store search results in findings
                        context.findings_registry[
                            "Fusionbase Search Results"] = f"Found {len(search_result)} organizations matching '{context.target_entity}'. Primary match: {first_result.get('name', 'Unknown')} (ID: {first_result['entity_id']})"

                        if verbose:
                            print(
                                f"    ✅ Found entity ID: {context.fusionbase_entity_id}"
                            )
                            print(
                                f"    📋 Organization name: {first_result.get('name', 'Unknown')}"
                            )
                    else:
                        if verbose:
                            print(
                                "    ⚠️ No valid entity ID found in search results"
                            )
                        return False
                else:
                    if verbose:
                        print("    ⚠️ No organizations found in search")
                    return False

            except Exception as e:
                if verbose:
                    print(f"    ❌ Organization search failed: {str(e)}")
                return False

        # Step 2: Organization Detail (if we have an entity ID and haven't done this)
        if context.fusionbase_entity_id and not context.fusionbase_detail_completed:
            if verbose:
                print(f"  📋 Retrieving detailed organization information...")

            try:
                detail_result = await execute_tool(
                    researcher_tool_map["organization_detail"],
                    "organization_detail", {
                        "entity_id": context.fusionbase_entity_id,
                        "client": fb_client
                    })

                context.fusionbase_detail_completed = True
                context.fusionbase_entity = detail_result

                # Store detailed information in findings
                if isinstance(detail_result, dict) and "name" in detail_result:
                    entity_summary = f"Organization: {detail_result['name']}"
                    if detail_result.get("primary_website"):
                        entity_summary += f"\nWebsite: {detail_result['primary_website']}"
                    if detail_result.get("country"):
                        entity_summary += f"\nCountry: {detail_result['country']}"
                    if detail_result.get("founding_date"):
                        entity_summary += f"\nFounding Date: {detail_result['founding_date']}"
                    if detail_result.get("contact"):
                        contact = detail_result["contact"]
                        if contact.get("email"):
                            entity_summary += f"\nEmail: {contact['email']}"
                        if contact.get("phone"):
                            entity_summary += f"\nPhone: {contact['phone']}"

                    context.findings_registry[
                        "Fusionbase Organization Details"] = entity_summary

                    # Add high-value insights
                    if detail_result.get("primary_website"):
                        context.key_insights.append(
                            f"Website: {detail_result['primary_website']}")

                    if verbose:
                        print(
                            f"    ✅ Retrieved detailed information for: {detail_result['name']}"
                        )
                else:
                    if verbose:
                        print("    ⚠️ Invalid detail result received")
                    return False

            except Exception as e:
                if verbose:
                    print(
                        f"    ❌ Organization detail retrieval failed: {str(e)}")
                return False

        if verbose:
            print("  🏛️ Fusionbase foundation established successfully")

        return True

    async def research_area_with_context(
            context: ResearchContext,
            area_description: str,
            system_content: Optional[str] = None,
            force_org_search: bool = False) -> Dict[str, Any]:
        """Research a specific area with full context awareness and shared Fusionbase foundation."""
        if verbose:
            print(f"\n📝 [RESEARCH] Investigating: '{area_description[:50]}...'")

        # Track this research area
        early_termination_stats["total_research_areas"] += 1

        # Build context-aware prompt with Fusionbase foundation information
        fusionbase_info = "None - Fusionbase lookup not completed yet"
        if context.fusionbase_entity:
            fusionbase_info = f"AVAILABLE: Organization details for {context.fusionbase_entity.get('name', 'Unknown')} (ID: {context.fusionbase_entity_id})"
            if context.fusionbase_entity.get("primary_website"):
                fusionbase_info += f", Website: {context.fusionbase_entity['primary_website']}"

        # Add schema field requirements if structured output is requested
        schema_fields_hint = ""
        if output_schema:
            schema_fields_hint = "\n\n🎯 STRUCTURED OUTPUT FIELDS TO RESEARCH:"
            if isinstance(output_schema, dict):
                properties = output_schema.get("properties", {})
                required_fields = output_schema.get("required", [])
                for field, spec in properties.items():
                    desc = spec.get("description", field)
                    req_marker = " (REQUIRED)" if field in required_fields else ""
                    schema_fields_hint += f"\n- {field}: {desc}{req_marker}"
            elif hasattr(output_schema, "__fields__"):
                field_names = list(output_schema.__fields__.keys())
                schema_fields_hint += f"\n- Fields needed: {', '.join(field_names)}"
            schema_fields_hint += "\n\nPrioritize finding information for these specific fields."

        context_info = f"""
RESEARCH CONTEXT:
- Original Query: {context.original_query}
- Research Goal: {context.research_goal}
- Target Entity: {context.target_entity}

CURRENT TASK: {area_description}

FUSIONBASE FOUNDATION DATA: {fusionbase_info}

PREVIOUSLY COMPLETED SEARCHES: {', '.join(context.completed_searches) if context.completed_searches else 'None'}

EXISTING FINDINGS:
{chr(10).join([f"- {name}: {content[:100]}..." for name, content in context.findings_registry.items()]) if context.findings_registry else 'None yet'}

KEY INSIGHTS SO FAR:
{chr(10).join([f"- {insight}" for insight in context.key_insights]) if context.key_insights else 'None yet'}{schema_fields_hint}

YOUR MISSION: Focus specifically on finding information for "{area_description}" that directly serves the research goal: "{context.research_goal}".

IMPORTANT NOTES:
- Fusionbase organization search and detail lookup have been completed globally
- You have access to the organization's basic information from Fusionbase
- Focus on finding ADDITIONAL information that complements what we already know
- Use web search for supplementary information not available in Fusionbase
- Use relation tools to explore connections and additional data points

EARLY TERMINATION STRATEGY:
- After each significant finding, assess if you have sufficient information for your research goal
- Use ResearchReflection tool with confidence 0.6+ if you believe the research is complete
- Don't over-research - efficiency is key
- Focus on getting the core information needed, not exhaustive coverage

You have up to {max_iterations} iterations to find comprehensive information. Use them strategically and terminate early when sufficient.
"""

        # Base research instructions
        base_research_content = RESEARCH_INSTRUCTIONS.format(
            section_description=area_description)

        # Append custom system content if provided
        full_research_content = base_research_content
        if system_content:
            full_research_content += f"\n\n{system_content}"

        messages = [
            SystemMessage(content=full_research_content),
            HumanMessage(content=context_info)
        ]

        # Research variables
        findings = {}
        iterations = 0
        last_response = ""
        tool_results = []
        search_page_tracking = {
        }  # Track which searches and pages we've explored
        early_termination = False
        sufficiency_reason = ""

        # No forced tool choices needed since foundation is established globally
        current_model = researcher_model_with_tools

        while iterations < max_iterations:
            iterations += 1

            if verbose:
                print(f"  ↪ Research iteration {iterations}/{max_iterations}")

            try:
                response = await current_model.ainvoke(messages)
                messages.append(response)
                last_response = response.content if hasattr(
                    response, "content") else ""

                if hasattr(response, "tool_calls") and response.tool_calls:
                    iteration_had_findings = False

                    for tool_call in response.tool_calls:
                        tool_name = tool_call["name"]
                        tool_call_id = tool_call["id"]
                        tool_args = dict(tool_call["args"])

                        if verbose:
                            arg_str = ", ".join([
                                f"{k}='{v}'"
                                if isinstance(v, str) else f"{k}={v}"
                                for k, v in tool_args.items()
                                if k not in ("client", "api_key", "proxies",
                                             "verify_ssl")
                            ])
                            print(f"  🔧 Tool: {tool_name}({arg_str})")

                        try:
                            # Add appropriate credentials and proxies
                            # Check if the tool requires a Fusionbase client by examining its parameters
                            tool_obj = researcher_tool_map.get(tool_name)
                            if tool_obj and hasattr(tool_obj, 'args_schema'):
                                # Check if the tool's schema includes a 'client' parameter
                                schema_fields = getattr(tool_obj.args_schema,
                                                        '__fields__', {})
                                if 'client' in schema_fields:
                                    tool_args["client"] = fb_client
                            # Fallback: check by tool name for backward compatibility
                            elif tool_name in (
                                    "organization_search",
                                    "organization_detail", "person_search",
                                    "person_detail", "relation_search",
                                    "relation_detail", "relation_resolve",
                                    "financial_kpi", "network",
                                    "related_persons",
                                    "profit_and_loss_account", "publications",
                                    "balance_sheet_accounts",
                                    "insolvency_publications",
                                    "annual_financial_statements", "news"):
                                tool_args["client"] = fb_client
                            # Google search specific handling
                            elif tool_name == "google_search":
                                tool_args["api_key"] = serp_api_key
                                # Google search doesn't need domain-specific proxies since it goes to ValueSERP API
                                if proxy_config and "*" in proxy_config:
                                    tool_args["proxies"] = proxy_config["*"]
                                tool_args["verify_ssl"] = verify_ssl

                                # Enhanced search tracking with pagination support
                                search_query = tool_args.get("query", "")
                                search_page = tool_args.get(
                                    "page", 1)  # Default to page 1
                                search_key = f"{search_query}:page{search_page}"

                                # Track this specific search + page combination
                                if search_key not in context.completed_searches:
                                    context.completed_searches.append(
                                        search_key)

                                # Update page tracking
                                if search_query not in search_page_tracking:
                                    search_page_tracking[search_query] = []
                                if search_page not in search_page_tracking[
                                        search_query]:
                                    search_page_tracking[search_query].append(
                                        search_page)

                                if verbose:
                                    print(
                                        f"    📄 Search page {search_page} for query: '{search_query}'"
                                    )

                            elif tool_name == "web_content":
                                # For web_content, determine the appropriate proxy based on the URL
                                url = tool_args.get("url", "")
                                if url and proxy_config:
                                    domain_proxy = _get_proxy_for_url(
                                        url, proxy_config)
                                    if domain_proxy:
                                        tool_args["proxies"] = domain_proxy
                                        if verbose:
                                            parsed_url = urlparse(url)
                                            domain = parsed_url.netloc
                                            http_proxy = domain_proxy.get(
                                                "http", "None")
                                            https_proxy = domain_proxy.get(
                                                "https", "None")
                                            print(
                                                f"    🌐 Using domain-specific proxy for {domain}: HTTP={http_proxy}, HTTPS={https_proxy}"
                                            )
                                tool_args["verify_ssl"] = verify_ssl

                            # Execute tool with deduplication
                            tool = researcher_tool_map[tool_name]
                            result = await execute_tool(tool, tool_name,
                                                        tool_args)

                            tool_results.append({
                                "tool_name": tool_name,
                                "args": tool_args,
                                "result": result,
                                "iteration": iterations
                            })

                            # Handle GlobalFinding results
                            if tool_name == "GlobalFinding":
                                section_name = tool_args.get(
                                    "section_name", "Untitled")
                                content = tool_args.get("content", "")
                                relevance_score = tool_args.get(
                                    "relevance_score", 0.5)
                                sources = tool_args.get("sources", [])

                                # Grade for hallucination
                                source_content = "\n\n".join([
                                    f"Tool: {tr['tool_name']}\nArgs: {tr['args']}\nResult: {str(tr['result'])}"
                                    for tr in tool_results[
                                        -5:]  # Use last 5 tool results for context
                                ])

                                grading_result = await grade_for_hallucination(
                                    claim=content,
                                    source_content=source_content,
                                    context=f"Research area: {area_description}"
                                )

                                # Only add if grounded and relevant (relaxed thresholds)
                                if grading_result.get(
                                        "is_grounded",
                                        False) and relevance_score > 0.4:
                                    # Store in global registry
                                    context.findings_registry[
                                        section_name] = content
                                    findings[section_name] = content
                                    iteration_had_findings = True

                                    # If high relevance, add to key insights (lowered threshold)
                                    if relevance_score > 0.6:
                                        insight = f"{section_name}: {content[:100]}..."
                                        if insight not in context.key_insights:
                                            context.key_insights.append(insight)

                                    if verbose:
                                        print(
                                            f"    ✅ Saved finding: '{section_name}' (relevance: {relevance_score:.2f})"
                                        )
                                else:
                                    if verbose:
                                        reason = "low relevance" if relevance_score <= 0.4 else "hallucinated"
                                        print(
                                            f"    ❌ Rejected finding: '{section_name}' ({reason})"
                                        )

                            # Handle ResearchReflection results
                            elif tool_name == "ResearchReflection":
                                is_sufficient = tool_args.get(
                                    "is_sufficient", False)
                                confidence = tool_args.get("confidence", 0.0)
                                reasoning = tool_args.get("reasoning", "")

                                # Only consider early termination if reasonably confident (lowered threshold)
                                if is_sufficient and confidence > 0.6:
                                    early_termination = True
                                    sufficiency_reason = reasoning

                                    if verbose:
                                        print(
                                            f"🏁 Research may be sufficient: {reasoning}"
                                        )

                                if verbose:
                                    status = "SUFFICIENT" if is_sufficient else "INSUFFICIENT"
                                    print(
                                        f"📊 Research reflection: {status} (confidence: {confidence:.2f})"
                                    )

                            # Create appropriate tool response based on the tool's response type
                            if token_handler.is_placeholder_response(result):
                                # Tool returned placeholder mode - use efficient handling
                                tool_response = token_handler.create_contextual_response(
                                    tool_name, result)
                            else:
                                # Regular response for tools that returned full data
                                tool_response = _create_contextual_tool_response(
                                    tool_name, result, tool_results,
                                    search_page_tracking, iterations)

                            messages.append(
                                ToolMessage(content=tool_response,
                                            name=tool_name,
                                            tool_call_id=tool_call_id))

                        except Exception as e:
                            error_msg = f"Error executing {tool_name}: {str(e)}"
                            messages.append(
                                ToolMessage(content=error_msg,
                                            name=tool_name,
                                            tool_call_id=tool_call_id))
                            if verbose:
                                print(f"    ❌ Error: {str(e)}")

                    # Check if we should terminate early AFTER processing all tool calls
                    if early_termination:
                        early_termination_stats["early_terminations"] += 1
                        early_termination_stats["termination_reasons"].append(
                            sufficiency_reason)
                        if verbose:
                            print(
                                f"🏁 Stopping research early - {sufficiency_reason}"
                            )
                        break

                    # Add guidance for next iteration if we haven't found enough yet
                    if not iteration_had_findings and iterations < max_iterations and not early_termination:
                        guidance = _create_iteration_guidance(
                            tool_results, search_page_tracking,
                            area_description, iterations)
                        messages.append(HumanMessage(content=guidance))

                        if verbose:
                            print(
                                f"    💡 Providing guidance for iteration {iterations + 1}"
                            )

                else:
                    # No tool calls - check if we should continue or wrap up
                    if iterations < max_iterations and not findings and not early_termination:
                        messages.append(
                            HumanMessage(content=(
                                f"You still have {max_iterations - iterations} iterations remaining. "
                                f"Continue searching for information about '{area_description}'. "
                                "Try different search strategies, visit web pages, or explore additional sources. "
                                "Use the tools available to find relevant information."
                            )))
                        if verbose:
                            print(
                                "  ↪ No tools called - encouraging continued search"
                            )
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
                    context=f"Research area: {area_description}")

                if grading_result.get("is_grounded", False):
                    findings["Summary"] = last_response
                    context.findings_registry[
                        f"{area_description} - Summary"] = last_response

        # Add completion summary to findings if research was terminated early
        if early_termination:
            completion_summary = f"Research for '{area_description}' was completed early after {iterations} iterations because: {sufficiency_reason}"
            context.findings_registry[
                f"{area_description} - Research Status"] = completion_summary
            findings["Research Status"] = completion_summary

        return {
            "area":
                area_description,
            "findings":
                findings,
            "iterations":
                iterations,
            "last_response":
                last_response,
            "search_pages_explored":
                search_page_tracking,
            "early_termination":
                early_termination,
            "termination_reason":
                sufficiency_reason
                if early_termination else "Max iterations reached or complete"
        }

    def _create_contextual_tool_response(tool_name: str, result: Any,
                                         tool_results: List[Dict],
                                         search_page_tracking: Dict,
                                         iteration: int) -> str:
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

    def _create_iteration_guidance(tool_results: List[Dict],
                                   search_page_tracking: Dict,
                                   area_description: str,
                                   current_iteration: int) -> str:
        """Create guidance for the next iteration based on what's been tried so far."""

        # Analyze what's been done
        tools_used = [tr["tool_name"] for tr in tool_results]
        searches_done = [
            tr for tr in tool_results if tr["tool_name"] == "google_search"
        ]
        web_content_extractions = [
            tr for tr in tool_results if tr["tool_name"] == "web_content"
        ]

        guidance_parts = [
            f"ITERATION {current_iteration + 1} GUIDANCE:",
            f"You're researching: '{area_description}'"
        ]

        # Suggest strategies based on what's been tried
        if "google_search" not in tools_used:
            guidance_parts.append(
                "• Consider starting with a Google search to find relevant information"
            )
        elif len(searches_done) == 1:
            guidance_parts.append(
                "• Try alternative search terms, or use the 'page' parameter to see more Google results (page=2, page=3, etc.)"
            )
        elif len(web_content_extractions) == 0 and searches_done:
            guidance_parts.append(
                "• You found search results but haven't extracted content from any pages. Use web_content on promising URLs"
            )
        else:
            guidance_parts.append(
                "• Try different search strategies: use synonyms, industry terms, or more specific queries"
            )

        # Add specific suggestions based on search tracking
        if search_page_tracking:
            for query, pages in search_page_tracking.items():
                max_page = max(pages)
                if max_page == 1:
                    guidance_parts.append(
                        f"• Consider exploring page 2+ for query '{query}' using page=2 parameter"
                    )

        guidance_parts.append(
            "• Don't give up - each iteration brings you closer to finding the information needed"
        )

        return "\n".join(guidance_parts)

    async def synthesize_final_report(
            context: ResearchContext,
            system_content: Optional[str] = None) -> Union[str, Dict[str, Any]]:
        """Synthesize all findings into a comprehensive final report or structured output."""
        if verbose:
            if output_schema:
                print("\n🔄 Synthesizing structured output")
            else:
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

        # Check if structured output is requested
        if output_schema:
            # Use structured output with schema
            if verbose:
                print("  📋 Using structured output schema")

            # Create instruction for structured output
            structured_prompt = f"""
{context_text}

Based on the research findings above, extract and structure the information according to the provided schema.
Be comprehensive and include all relevant information that fits the schema structure.
Only include fields that are defined in the schema - do not add extra fields.
If a required field is not found in the research, use null or an appropriate empty value.
"""

            try:
                # Ensure schema has required top-level fields for LangChain
                schema_to_use = output_schema
                if isinstance(output_schema, dict):
                    # Add title and description if missing
                    if "title" not in output_schema:
                        schema_to_use = {
                            "title":
                                "StructuredOutput",
                            "description":
                                "Structured information extracted from research findings",
                            **output_schema
                        }

                # Use with_structured_output for the synthesizer model
                structured_model = synthesizer_model.with_structured_output(
                    schema_to_use)
                # Base structured output instructions
                base_structured_content = "You are a research synthesizer. Extract and structure information from research findings according to the provided schema. Only return data in the exact structure requested."

                # Append custom system content if provided
                full_structured_content = base_structured_content
                if system_content:
                    full_structured_content += f"\n\n{system_content}"

                result = await structured_model.ainvoke([
                    SystemMessage(content=full_structured_content),
                    HumanMessage(content=structured_prompt)
                ])

                if verbose:
                    print("  ✅ Structured output generated successfully")

                return result
            except Exception as e:
                if verbose:
                    print(f"  ❌ Error generating structured output: {str(e)}")
                    print(f"  📝 Error details: {type(e).__name__}")

                # For structured output, we should NOT fall back to regular synthesis
                # Instead, return an error or empty structure
                if isinstance(output_schema, dict):
                    # Return empty JSON structure with required fields
                    properties = output_schema.get("properties", {})
                    empty_result = {}
                    for field, spec in properties.items():
                        field_type = spec.get("type", "string")
                        if field_type == "array":
                            empty_result[field] = []
                        elif field_type == "object":
                            empty_result[field] = {}
                        elif field_type == "number":
                            empty_result[field] = 0
                        elif field_type == "boolean":
                            empty_result[field] = False
                        else:
                            empty_result[field] = None
                    return empty_result
                else:
                    # For other schema types, raise the error
                    raise e

        # Regular synthesis (non-structured)
        # Base synthesis instructions
        base_synthesis_content = SYNTHESIS_INSTRUCTIONS

        # Append custom system content if provided
        full_synthesis_content = base_synthesis_content
        if system_content:
            full_synthesis_content += f"\n\n{system_content}"

        messages = [
            SystemMessage(content=full_synthesis_content),
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
                        format_type = tool_args.get('format_type',
                                                    'intelligent')
                        print(f"  🔧 Using: {tool_name} (format: {format_type})")

                    if tool_name == "FinalAnswer":
                        final_content = tool_args.get("content", "")

                        if verbose:
                            print(f"  ✅ Final answer synthesized intelligently")

                        return final_content

                    # Handle legacy tools for backward compatibility
                    elif tool_name in ["Introduction", "Conclusion"]:
                        tool = synthesizer_tool_map[tool_name]
                        result = await execute_tool(tool, tool_name, tool_args)

                        if not final_content:
                            # If no FinalAnswer was provided, fall back to traditional format
                            if tool_name == "Introduction":
                                intro_content = f"# {result.name}\n\n{result.content}"
                            elif tool_name == "Conclusion":
                                conclusion_content = f"## {result.name}\n\n{result.content}"

                # If we got legacy format, assemble traditional report
                if not final_content and ('intro_content' in locals() or
                                          'conclusion_content' in locals()):
                    body_parts = []
                    for section_name, content in context.findings_registry.items(
                    ):
                        body_parts.append(f"## {section_name}\n\n{content}")

                    body_content = "\n\n".join(body_parts)
                    intro = locals().get(
                        'intro_content',
                        f"# Research Report: {context.target_entity}")
                    conclusion = locals().get(
                        'conclusion_content', "## Summary\nResearch completed.")
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

        parts.append(
            f"## Summary\nResearch completed with {len(context.findings_registry)} findings."
        )

        return "\n".join(parts)

    async def ainvoke(initial_state):
        """Execute the research agent process with context awareness."""

        # Reset tool call tracking for this run
        seen_tool_calls.clear()
        tool_call_results.clear()

        # Reset early termination stats for this run
        early_termination_stats["total_research_areas"] = 0
        early_termination_stats["early_terminations"] = 0
        early_termination_stats["termination_reasons"] = []

        # Extract messages and separate system/user content
        messages = initial_state.get("messages", [])
        if not messages:
            raise ValueError("No messages provided")

        # Extract system and user messages
        system_content = None
        user_query = None

        for msg in messages:
            if isinstance(msg, dict):
                role = msg.get("role", "")
                content = msg.get("content", "")
            elif hasattr(msg, "role") and hasattr(msg, "content"):
                role = msg.role
                content = msg.content
            else:
                continue

            if role == "system" and content:
                system_content = content
            elif role == "user" and content and not user_query:
                user_query = content

        if not user_query:
            raise ValueError("No user query provided")

        # Extract target entity from query with improved extraction using LLM
        target_entity = initial_state.get("company_topic",
                                          "the requested topic")
        if target_entity == "the requested topic":
            target_entity = await extract_clean_company_name(user_query)

        if verbose:
            print(f"\n🚀 Starting context-aware research")
            print(f"🎯 Target Entity: {target_entity}")
            print(f"🔎 Query: '{user_query}'")

        try:
            # PHASE 1: Create research context and plan
            research_context = await create_research_plan(
                user_query, target_entity, system_content)

            # PHASE 2: Establish Fusionbase foundation BEFORE research areas
            foundation_success = await establish_fusionbase_foundation(
                research_context)
            if not foundation_success:
                if verbose:
                    print(
                        "⚠️ Could not establish Fusionbase foundation - proceeding with web-only research"
                    )

            # PHASE 3: Execute research areas with shared foundation
            if verbose:
                print(
                    f"\n🔍 Executing research with {len(research_context.research_plan.key_questions)} questions and {len(research_context.research_plan.data_points)} data points"
                )

            research_areas = []
            for question in research_context.research_plan.key_questions:
                research_areas.append(f"Question: {question}")
            for data_point in research_context.research_plan.data_points:
                research_areas.append(f"Data Point: {data_point}")

            if not research_areas:
                research_areas = ["Basic company information"]

            # Execute research with context awareness (no forced org search needed)
            research_tasks = []
            for area in research_areas:
                research_tasks.append(
                    research_area_with_context(research_context,
                                               area,
                                               system_content,
                                               force_org_search=False))

            await asyncio.gather(*research_tasks)

            # PHASE 4: Generate final report
            final_report = await synthesize_final_report(
                research_context, system_content)

            if verbose:
                print(f"\n🏁 Research complete")
                print(
                    f"📊 Total findings: {len(research_context.findings_registry)}"
                )
                print(f"💡 Key insights: {len(research_context.key_insights)}")
                print(
                    f"🔍 Searches completed: {len(research_context.completed_searches)}"
                )

                # Log early termination statistics
                total_areas = early_termination_stats["total_research_areas"]
                early_terms = early_termination_stats["early_terminations"]
                print(
                    f"⏱️ Early terminations: {early_terms}/{total_areas} research areas completed early"
                )
                if early_terms > 0:
                    efficiency_pct = (early_terms / total_areas) * 100
                    print(
                        f"⚡ Research efficiency: {efficiency_pct:.1f}% of areas completed ahead of schedule"
                    )
                    if verbose and early_termination_stats[
                            "termination_reasons"]:
                        print(
                            f"📝 Termination reasons: {', '.join(early_termination_stats['termination_reasons'][:3])}{'...' if len(early_termination_stats['termination_reasons']) > 3 else ''}"
                        )

            # Apply placeholder replacement if any placeholders were used
            final_report_processed = token_handler.replace_placeholders(
                final_report)

            # Clear the token handler for next run
            token_handler.clear()

            return final_report_processed

        except Exception as e:
            if verbose:
                print(f"\n❌ Error in research: {str(e)}")
                import traceback
                traceback.print_exc()

            return f"# Research Error\n\nAn error occurred while researching {target_entity}: {str(e)}"

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
