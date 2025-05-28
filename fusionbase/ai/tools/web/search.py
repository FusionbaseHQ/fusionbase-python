"""Web search tools for Fusionbase AI."""

import os
from typing import Any, Dict, Optional

import httpx
from typing_extensions import Annotated

try:
    from langchain_core.tools import InjectedToolArg
    from langchain_core.tools import tool
except ImportError:
    raise ImportError(
        "Could not import langchain package. "
        "Please install the required dependencies: "
        "pip install fusionbase[ai] "
        "or "
        "pip install langchain>=0.3.0 langchain-core>=0.3.0"
    )


@tool
def google_search(
    query: Annotated[str, "The search query to execute on Google"],
    hl: Annotated[Optional[str], "The language parameter for the UI (e.g., 'en', 'de', 'fr')"] = None,
    gl: Annotated[Optional[str], "The country parameter to limit results (e.g., 'us', 'de', 'uk')"] = None,
    page: Annotated[Optional[int], "Page number for pagination (1 for first page, 2 for second, etc.). Default is 1"] = 1,
    api_key: Annotated[str, InjectedToolArg] = None,
    verify_ssl: Annotated[Optional[bool], "Whether to verify SSL certificates"] = True,
) -> Dict[str, Any]:
    """Search Google and get relevant search results with pagination support.

    This tool performs a Google search and returns the most relevant results.
    Use it to find current information on the web. You can paginate through results
    using the 'page' parameter to explore more comprehensive results.

    Example queries:
    - "latest developments in AI"
    - "weather in Berlin Germany"
    - "who is the CEO of Microsoft"

    Pagination examples:
    - page=1 (default): First 10 results
    - page=2: Results 11-20
    - page=3: Results 21-30
    """
    # Check for API key
    if not api_key:
        api_key = os.environ.get("SERP_API_KEY")

    if not api_key:
        return {
            "error": "SERP API key is required. Please provide it via api_key parameter or set the SERP_API_KEY environment variable."
        }

    base_url = "https://api.valueserp.com/search"

    # Build parameters for the API request
    params = {
        "api_key": api_key,
        "q": query
    }

    if hl:
        params["hl"] = hl

    if gl:
        params["gl"] = gl

    # Add page parameter for pagination
    if page and page > 1:
        params["page"] = page

    # Make the request to the SERP API
    try:
        # ValueSERP API calls should not use proxies
        client_kwargs = {"timeout": 30.0, "verify": verify_ssl}

        with httpx.Client(**client_kwargs) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()  # Raise exception for non-200 responses
            data = response.json()
    except httpx.HTTPStatusError as e:
        return {
            "error": f"API request failed with status {e.response.status_code}",
            "details": e.response.text
        }
    except httpx.RequestError as e:
        return {
            "error": f"Request failed: {str(e)}"
        }
    except Exception as e:
        return {
            "error": f"Unexpected error: {str(e)}"
        }

    # Check if request was successful based on request_info
    if 'request_info' in data and not data['request_info'].get('success', False):
        return {
            "error": "API request unsuccessful",
            "details": data.get('request_info', {})
        }

    # Process and format the results
    results = {
        "query": query,
        "page": page
    }

    # Include knowledge graph and organic results directly as they come from the API
    if 'knowledge_graph' in data:
        results["knowledge_graph"] = data['knowledge_graph']

    if 'organic_results' in data:
        results["organic_results"] = data['organic_results']

    return results


@tool
async def async_google_search(
    query: Annotated[str, "The search query to execute on Google"],
    hl: Annotated[Optional[str], "The language parameter for the UI (e.g., 'en', 'de', 'fr')"] = None,
    gl: Annotated[Optional[str], "The country parameter to limit results (e.g., 'us', 'de', 'uk')"] = None,
    page: Annotated[Optional[int], "Page number for pagination (1 for first page, 2 for second, etc.). Default is 1"] = 1,
    api_key: Annotated[str, InjectedToolArg] = None,
    verify_ssl: Annotated[Optional[bool], "Whether to verify SSL certificates"] = True,
) -> Dict[str, Any]:
    """Search Google asynchronously and get relevant search results with pagination support.

    This tool performs a Google search and returns the most relevant results.
    Use it to find current information on the web. You can paginate through results
    using the 'page' parameter to explore more comprehensive results.

    Example queries:
    - "latest developments in AI"
    - "weather in Berlin Germany"
    - "who is the CEO of Microsoft"

    Pagination examples:
    - page=1 (default): First 10 results
    - page=2: Results 11-20
    - page=3: Results 21-30
    """
    # Check for API key
    if not api_key:
        api_key = os.environ.get("SERP_API_KEY")

    if not api_key:
        return {
            "error": "SERP API key is required. Please provide it via api_key parameter or set the SERP_API_KEY environment variable."
        }

    base_url = "https://api.valueserp.com/search"

    # Build parameters for the API request
    params = {
        "api_key": api_key,
        "q": query
    }

    if hl:
        params["hl"] = hl

    if gl:
        params["gl"] = gl

    # Add page parameter for pagination
    if page and page > 1:
        params["page"] = page

    # Make the request to the SERP API
    try:
        # ValueSERP API calls should not use proxies
        client_kwargs = {"timeout": 30.0, "verify": verify_ssl}

        async with httpx.AsyncClient(**client_kwargs) as client:
            response = await client.get(base_url, params=params)
            response.raise_for_status()  # Raise exception for non-200 responses
            data = response.json()
    except httpx.HTTPStatusError as e:
        return {
            "error": f"API request failed with status {e.response.status_code}",
            "details": e.response.text
        }
    except httpx.RequestError as e:
        return {
            "error": f"Request failed: {str(e)}"
        }
    except Exception as e:
        return {
            "error": f"Unexpected error: {str(e)}"
        }

    # Check if request was successful based on request_info
    if 'request_info' in data and not data['request_info'].get('success', False):
        return {
            "error": "API request unsuccessful",
            "details": data.get('request_info', {})
        }

    # Process and format the results
    results = {
        "query": query,
        "page": page
    }

    # Include knowledge graph and organic results directly as they come from the API
    if 'knowledge_graph' in data:
        results["knowledge_graph"] = data['knowledge_graph']

    if 'organic_results' in data:
        results["organic_results"] = data['organic_results']

    return results
