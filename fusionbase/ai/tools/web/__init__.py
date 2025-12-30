"""Web tools module for Fusionbase AI."""

try:
    from .content import async_web_content
    from .content import web_content
    from .search import async_google_search
    from .search import google_search

    __all__ = [
        "google_search", "async_google_search", "web_content",
        "async_web_content"
    ]
except ImportError:
    # This will happen if langchain or html2text is not installed
    __all__ = []
