#!/usr/bin/env python
"""
A minimalistic CLI agent that uses Fusionbase tools to provide company information.
"""

import os
import sys

# Check for required environment variables
if not os.environ.get("OPENAI_API_KEY"):
    print("Error: OPENAI_API_KEY environment variable is not set")
    print("Please export OPENAI_API_KEY=your_key_here")
    sys.exit(1)

if not os.environ.get("FUSIONBASE_API_KEY"):
    print("Error: FUSIONBASE_API_KEY environment variable is not set")
    print("Please export FUSIONBASE_API_KEY=your_key_here")
    sys.exit(1)

# Check for optional SERP_API_KEY
serp_api_key = os.environ.get("SERP_API_KEY")
if not serp_api_key:
    print("Warning: SERP_API_KEY environment variable is not set.")
    print("Web search functionality will be limited.")
    print("To enable web search, export SERP_API_KEY=your_key_here")

try:
    from langchain_core.messages import HumanMessage
    from langchain_core.messages import SystemMessage
    from langchain_core.messages import ToolMessage
    from langchain_openai import ChatOpenAI

    from fusionbase import Fusionbase
    from fusionbase.ai.tools.entity.organization import organization_detail
    from fusionbase.ai.tools.entity.organization import organization_search
    from fusionbase.ai.tools.web.content import web_content
    from fusionbase.ai.tools.web.search import google_search
except ImportError as e:
    print(f"Error importing required packages: {e}")
    print("Please install all required packages:")
    print("pip install fusionbase[ai]")
    sys.exit(1)


def setup_agent():
    """Initialize the Fusionbase client, tools, and chat model."""
    # Initialize Fusionbase client
    fb_client = Fusionbase()

    # Initialize OpenAI chat model
    model = ChatOpenAI(
        model="gpt-4o",
        temperature=0
    )

    # Define tools - include web tools
    tools = [organization_search, organization_detail, google_search, web_content]

    # Create a system message for the agent
    system_message = SystemMessage(content="""
    You are a helpful assistant that provides information about companies.

    You have the following tools available:

    1. COMPANY DATABASE TOOLS:
       - organization_search: Search for companies by name in the Fusionbase database
       - organization_detail: Get detailed information about a company using its entity_id from search results

    2. WEB TOOLS:
       - google_search: Search the web for information not available in the database
       - web_content: Retrieve and analyze specific web pages for detailed information

    WORKFLOW:
    1. First try using organization_search to find the company in the Fusionbase database
    2. Then use organization_detail to get structured information about the company
    3. If database information is insufficient, use google_search to find relevant websites
    4. Use web_content to extract information from specific pages found in search results

    Always present the information in a clear, concise manner. Cite your sources.
    """)

    # Bind tools to the model
    model_with_tools = model.bind_tools(tools)

    return fb_client, model_with_tools, tools, system_message


def run_cli():
    """Run the CLI loop."""
    print("🏢 Fusionbase Company Information Agent 🤖")
    print("Ask me anything about companies! (type 'exit' to quit)")
    print("Example questions:")
    print("- Tell me about Microsoft Corporation")
    print("- What does BMW AG do?")
    print("- When was Apple Inc founded?")
    print("- Compare Tesla and Rivian")
    print()

    fb_client, model, tools, system_message = setup_agent()
    tool_map = {tool.name: tool for tool in tools}

    # Get SERP API key for web search
    serp_api_key = os.environ.get("SERP_API_KEY")

    while True:
        try:
            # Get user input
            query = input("🔍 Query: ")
            if query.lower() in ("exit", "quit", "q"):
                print("Goodbye! 👋")
                break

            print("🧠 Thinking...")

            # Set up conversation with system message and user query
            messages = [
                system_message,
                HumanMessage(content=query)
            ]

            # Loop for multi-step tool calling
            tool_call_round = 1
            while True:
                # Get model response
                response = model.invoke(messages)

                # Add response to message history
                messages.append(response)

                # Check if the model called any tools
                if hasattr(response, "tool_calls") and response.tool_calls:
                    print(f"\n🔧 Executing tools (round {tool_call_round})...")

                    # Execute each tool and create proper tool messages
                    for tool_call in response.tool_calls:
                        tool_name = tool_call["name"]
                        tool_args = dict(tool_call["args"])
                        tool_call_id = tool_call["id"]

                        # Display tool parameters without credentials
                        visible_args = {k: v for k, v in tool_args.items()
                                       if k not in ('client', 'api_key', 'proxy_url')}
                        print(f"- Running: {tool_name} with parameters: {', '.join(f'{k}={v}' for k, v in visible_args.items())}")

                        # Add appropriate credentials based on tool
                        if tool_name in ("organization_search", "organization_detail"):
                            tool_args["client"] = fb_client
                        elif tool_name == "google_search" and serp_api_key:
                            tool_args["api_key"] = serp_api_key

                        # Execute the tool with arguments
                        selected_tool = tool_map[tool_name]
                        result = selected_tool.invoke(tool_args)

                        # Create a tool message with the correct tool_call_id
                        tool_message = ToolMessage(
                            content=str(result),
                            name=tool_name,
                            tool_call_id=tool_call_id
                        )

                        # Add tool message to conversation history
                        messages.append(tool_message)

                    tool_call_round += 1
                    print("🔄 Continuing with more tool calls if needed...")
                else:
                    # Model didn't call any more tools - we have our final response
                    print("\n🤖 Response:")
                    print(response.content)
                    break

        except KeyboardInterrupt:
            print("\nGoodbye! 👋")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()

        print("\n" + "-" * 50)


if __name__ == "__main__":
    run_cli()
