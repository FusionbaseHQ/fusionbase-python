#!/usr/bin/env python
"""Example demonstrating the Company Research Agent with Fusionbase tools."""

import argparse
import asyncio
import os
from pathlib import Path
import sys

# Check for required environment variables
if not os.environ.get("OPENAI_API_KEY"):
    print("Error: OPENAI_API_KEY environment variable not set.")
    print("Please set it with: export OPENAI_API_KEY=your_key_here")
    sys.exit(1)

if not os.environ.get("FUSIONBASE_API_KEY"):
    print("Error: FUSIONBASE_API_KEY environment variable not set.")
    print("Please set it with: export FUSIONBASE_API_KEY=your_key_here")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.markdown import Markdown

    from fusionbase import Fusionbase
    from fusionbase.ai.agents import create_company_research_agent
except ImportError as e:
    print(f"Error importing required packages: {e}")
    print("Please install all required dependencies:")
    print("pip install fusionbase[ai] rich 'langgraph>=0.0.16'")
    sys.exit(1)

# pylint: disable=too-many-locals, too-many-statements
async def main_async(args):
    """Run the company research agent example asynchronously."""
    console = Console()
    console.print("\n[bold blue]Fusionbase Research Agent[/bold blue]\n")

    # Determine the research topic and query
    if args.query:
        query = args.query
        console.print(f"Researching query: [bold yellow]{query}[/bold yellow]\n")
    else:
        # Fall back to company-specific query if no direct query is provided
        query = f"Research {args.company} and create a comprehensive company report."
        console.print(f"Researching company: [bold yellow]{args.company}[/bold yellow]\n")

    # Initialize clients
    console.print("Initializing clients...", end="")
    fb_client = Fusionbase()
    console.print(" [green]Done[/green]")

    # Process proxy settings if provided
    proxies = None
    if args.proxy:
        console.print(f"Using proxy: {args.proxy}")
        proxies = {"http": args.proxy, "https": args.proxy}

    # Create the research agent
    console.print("Setting up research agent...", end="")
    serp_api_key = os.environ.get("SERP_API_KEY")
    if not serp_api_key:
        console.print(
            "\n[yellow]Warning: SERP_API_KEY not set. Web search functionality will be limited.[/yellow]"
        )
        console.print("[yellow]Set the API key with: export SERP_API_KEY=your_key_here[/yellow]\n")

    # Create the agent with optional proxy settings and SSL verification
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        supervisor_model=args.model,
        researcher_model=args.model,
        serp_api_key=serp_api_key,
        max_iterations=args.max_iterations,
        verbose=args.verbose,
        proxies=proxies,
        verify_ssl=not args.no_verify_ssl  # Pass SSL verification setting
    )
    console.print(" [green]Done[/green]")

    # Create status display
    status_display = "Conducting research"
    if args.verbose:
        status_display += " (use --verbose to see detailed progress)"

    with console.status(f"[bold green]{status_display}...") as status:
        try:
            # Initialize state with only the query - no separate company_topic
            initial_state = {
                "messages": [{"role": "user", "content": query}]
            }

            # Use ainvoke instead of invoke for async execution
            result = await agent.ainvoke(initial_state)
            status.update("[bold green]Research complete! Generating report...")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            console.print(f"\n[bold red]Error during research: {str(e)}[/bold red]")
            if args.verbose:
                import traceback  # pylint: disable=import-outside-toplevel
                traceback.print_exc()
            sys.exit(1)

    # Extract the final report
    report = result.get("final_report", "No report generated.")

    # Output the report
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        console.print(f"\n[green]Report saved to {output_path}[/green]")
    else:
        console.print("\n[bold]Research Report:[/bold]\n")
        console.print(Markdown(report))

    console.print("\n[bold green]Research completed successfully[/bold green]")

def main():
    """Run the company research agent example."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Generate research reports or answer specific questions")

    # Create mutually exclusive group for query vs company
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument("--company", help="Name of the company to research")
    query_group.add_argument("--query", help="Custom research query (e.g., 'What is the LinkedIn URL of Fusionbase?')")

    parser.add_argument("--model", default="gpt-4o", help="Model to use for research (default: gpt-4o)")
    parser.add_argument("--output", help="Output file path to save the report (default: stdout)")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output during processing")
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=10,
        help="Maximum iterations before forcing completion (default: 10)"
    )
    parser.add_argument("--proxy", help="HTTP proxy URL to use for web requests")
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification when using proxies"
    )
    args = parser.parse_args()

    # Run the async main function
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
