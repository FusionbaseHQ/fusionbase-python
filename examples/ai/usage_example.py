#!/usr/bin/env python
"""Example showing how to use the Company Research Agent programmatically."""

import asyncio
import os

from langchain_openai import ChatOpenAI

from fusionbase import Fusionbase
from fusionbase.ai.agents import create_company_research_agent


async def main():
    """Example of using the company research agent with different proxy configurations."""

    # Initialize clients
    fb_client = Fusionbase()

    # Create model instances
    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # Example 1: Simple proxy configuration
    simple_proxies = {
        "http": "http://proxy:8080",
        "https": "https://proxy:8080"
    }

    # Example 2: Domain-specific proxy configuration
    domain_specific_proxies = {
        "*": {
            "http": "http://default-proxy:8080",
            "https": "https://default-proxy:8080"
        },
        "linkedin.com": {
            "http": "http://linkedin-proxy:8080",
            "https": "https://linkedin-proxy:8080"
        },
        "*.linkedin.com": {
            "http": "http://linkedin-proxy:8080",
            "https": "https://linkedin-proxy:8080"
        },
        "github.com": {
            "http": "http://github-proxy:8080",
            "https": "https://github-proxy:8080"
        }
    }

    # Example 3: No proxy
    no_proxy = None

    # Choose which proxy configuration to use
    proxy_config = domain_specific_proxies  # Change this as needed

    # Create the agent with proxy configuration
    agent = create_company_research_agent(
        fusionbase_client=fb_client,
        planner_model=model,
        researcher_model=model,
        synthesizer_model=model,
        hallucination_grader_model=model,
        serp_api_key=os.environ.get("SERP_API_KEY"),
        max_iterations=5,
        verbose=True,
        proxies=proxy_config,  # Pass proxy configuration directly
        verify_ssl=False  # Disable SSL verification if using proxies that require it
    )

    # Run a research query
    query = "What is MediaMir GmbH's LinkedIn URL?"
    initial_state = {
        "messages": [{"role": "user", "content": query}]
    }

    result = await agent.ainvoke(initial_state)
    print(result["final_report"])

if __name__ == "__main__":
    asyncio.run(main())
