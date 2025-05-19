"""Web content retrieval tools for Fusionbase AI."""

import os
import re
from typing import Any, Dict, Optional
from urllib.parse import urljoin
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import html2text
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
def web_content(
    url: Annotated[str, "The URL of the web page to retrieve"],
    proxy_url: Annotated[Optional[str], InjectedToolArg] = None
) -> Dict[str, Any]:
    """Retrieve and process content from a web page, converting it to markdown.

    This tool fetches a web page, cleans it by removing unnecessary elements like
    scripts and ads, and converts the content to markdown format. All relative
    links are converted to absolute URLs.

    Example usage:
    - Get content from a company's homepage
    - Retrieve an article from a news site
    - Extract information from a documentation page
    """
    # Validate URL
    if not url.startswith(('http://', 'https://')):
        return {"error": "Invalid URL. Must start with http:// or https://"}

    # Set up httpx client with proxy if provided
    client_kwargs = {"timeout": 30.0, "follow_redirects": True}
    if proxy_url:
        client_kwargs["proxies"] = {"all": proxy_url}

    try:
        # Make the request
        with httpx.Client(**client_kwargs) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = client.get(url, headers=headers)
            response.raise_for_status()

            # Get content type
            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" not in content_type:
                return {
                    "error": f"URL returned non-HTML content: {content_type}",
                    "content": response.text[:1000] if len(response.text) > 1000 else response.text
                }

            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Clean the HTML
            for element in soup(["script", "style", "iframe", "nav", "footer", "ad", "ads"]):
                element.decompose()

            # Extract title
            title = soup.title.text if soup.title else ""

            # Convert all relative links to absolute
            for a_tag in soup.find_all('a', href=True):
                if not a_tag['href'].startswith(('http://', 'https://')):
                    a_tag['href'] = urljoin(url, a_tag['href'])

            # Convert HTML to Markdown using html2text
            h = html2text.HTML2Text()
            h.ignore_images = False
            h.ignore_links = False
            h.body_width = 0  # No wrapping
            h.unicode_snob = True  # Use Unicode instead of ASCII

            # Convert to markdown
            markdown_content = h.handle(str(soup))

            # Create the result
            result = {
                "title": title.strip(),
                "content": markdown_content.strip(),
                "url": url
            }

            return result

    except httpx.HTTPStatusError as e:
        return {
            "error": f"HTTP error: {e.response.status_code}",
            "details": e.response.text[:500] if hasattr(e.response, "text") else str(e)
        }
    except httpx.RequestError as e:
        return {
            "error": f"Request error: {str(e)}"
        }
    except Exception as e:
        return {
            "error": f"Error processing page: {str(e)}"
        }


@tool
async def async_web_content(
    url: Annotated[str, "The URL of the web page to retrieve"],
    proxy_url: Annotated[Optional[str], InjectedToolArg] = None
) -> Dict[str, Any]:
    """Asynchronously retrieve and process content from a web page, converting it to markdown.

    This tool fetches a web page, cleans it by removing unnecessary elements like
    scripts and ads, and converts the content to markdown format. All relative
    links are converted to absolute URLs.

    Example usage:
    - Get content from a company's homepage
    - Retrieve an article from a news site
    - Extract information from a documentation page
    """
    # Validate URL
    if not url.startswith(('http://', 'https://')):
        return {"error": "Invalid URL. Must start with http:// or https://"}

    # Set up httpx client with proxy if provided
    client_kwargs = {"timeout": 30.0, "follow_redirects": True}
    if proxy_url:
        client_kwargs["proxies"] = {"all": proxy_url}

    try:
        # Make the request
        async with httpx.AsyncClient(**client_kwargs) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = await client.get(url, headers=headers)
            response.raise_for_status()

            # Get content type
            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" not in content_type:
                return {
                    "error": f"URL returned non-HTML content: {content_type}",
                    "content": response.text[:1000] if len(response.text) > 1000 else response.text
                }

            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Clean the HTML
            for element in soup(["script", "style", "iframe", "nav", "footer", "ad", "ads"]):
                element.decompose()

            # Extract title
            title = soup.title.text if soup.title else ""

            # Convert all relative links to absolute
            for a_tag in soup.find_all('a', href=True):
                if not a_tag['href'].startswith(('http://', 'https://')):
                    a_tag['href'] = urljoin(url, a_tag['href'])

            # Convert HTML to Markdown using html2text
            h = html2text.HTML2Text()
            h.ignore_images = False
            h.ignore_links = False
            h.body_width = 0  # No wrapping
            h.unicode_snob = True  # Use Unicode instead of ASCII

            # Convert to markdown
            markdown_content = h.handle(str(soup))

            # Create the result
            result = {
                "title": title.strip(),
                "content": markdown_content.strip(),
                "url": url
            }

            return result

    except httpx.HTTPStatusError as e:
        return {
            "error": f"HTTP error: {e.response.status_code}",
            "details": e.response.text[:500] if hasattr(e.response, "text") else str(e)
        }
    except httpx.RequestError as e:
        return {
            "error": f"Request error: {str(e)}"
        }
    except Exception as e:
        return {
            "error": f"Error processing page: {str(e)}"
        }
