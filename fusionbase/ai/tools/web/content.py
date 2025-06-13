"""Web content extraction tools for Fusionbase AI."""

from typing import Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import html2text
import httpx
from typing_extensions import Annotated

try:
    from langchain_core.tools import InjectedToolArg
    from langchain_core.tools import tool
except ImportError as exc:
    raise ImportError(
        "Could not import langchain package. "
        "Please install the required dependencies: "
        "pip install fusionbase[ai] "
        "or "
        "pip install langchain>=0.3.0 langchain-core>=0.3.0"
    ) from exc


@tool
def web_content(
    url: Annotated[str, "The URL of the web page to extract content from"],
    proxies: Annotated[Optional[Dict[str, str]], "HTTP proxies to use for this specific request"] = None,
    verify_ssl: Annotated[Optional[bool], "Whether to verify SSL certificates"] = True,
) -> str:
    """Extract and process the main content from a webpage.

    This tool fetches web pages and intelligently extracts the main content,
    removing navigation, ads, footers and other irrelevant content.

    The proxies parameter should be in the format:
    {"http": "http://proxy:8080", "https": "https://proxy:8080"}

    Returns the extracted content in a clean, readable format.
    """
    try:
        # Configure httpx client with proxies if provided
        client_kwargs = {
            "headers": {"User-Agent": "Mozilla/5.0 Fusionbase/1.0"},
            "follow_redirects": True,
            "timeout": 15.0
        }
        if proxies:
            client_kwargs["proxies"] = proxies
        # Allow disabling SSL verification when needed for certain proxies
        client_kwargs["verify"] = verify_ssl

        # Fetch the page
        with httpx.Client(**client_kwargs) as client:
            response = client.get(url)
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

    except Exception as e:
        return f"Error extracting content from {url}: {str(e)}"


@tool
async def async_web_content(
    url: Annotated[str, "The URL of the web page to extract content from"],
    proxies: Annotated[Optional[Dict[str, str]], "HTTP proxies to use for this specific request"] = None,
    verify_ssl: Annotated[Optional[bool], "Whether to verify SSL certificates"] = True,
) -> str:
    """Extract and process the main content from a webpage asynchronously.

    This tool fetches web pages and intelligently extracts the main content,
    removing navigation, ads, footers and other irrelevant content.

    The proxies parameter should be in the format:
    {"http": "http://proxy:8080", "https": "https://proxy:8080"}

    Returns the extracted content in a clean, readable format.
    """
    try:
        # Configure httpx client with proxies if provided
        client_kwargs = {
            "headers": {"User-Agent": "Mozilla/5.0 Fusionbase/1.0"},
            "follow_redirects": True,
            "timeout": 15.0
        }
        if proxies:
            client_kwargs["proxies"] = proxies
        # Allow disabling SSL verification when needed for certain proxies
        client_kwargs["verify"] = verify_ssl

        # Fetch the page asynchronously
        async with httpx.AsyncClient(**client_kwargs) as client:
            response = await client.get(url)
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

    except Exception as e:
        return f"Error extracting content from {url}: {str(e)}"
