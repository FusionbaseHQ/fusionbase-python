"""Tools for Fusionbase AI."""

try:
    from .entity.organization import async_organization_detail
    from .entity.organization import async_organization_search
    from .entity.organization import organization_detail
    from .entity.organization import organization_search
    from .entity.relation import async_relation_detail
    from .entity.relation import async_relation_resolve
    from .entity.relation import async_relation_search
    from .entity.relation import relation_detail
    from .entity.relation import relation_resolve
    from .entity.relation import relation_search
    from .relation.convenience import annual_financial_statements
    from .relation.convenience import balance_sheet_accounts
    from .relation.convenience import financial_kpi
    from .relation.convenience import insolvency_publications
    from .relation.convenience import network
    from .relation.convenience import news
    from .relation.convenience import profit_and_loss_account
    from .relation.convenience import publications
    from .relation.convenience import related_persons
    from .web.content import async_web_content
    from .web.content import web_content
    from .web.search import async_google_search
    from .web.search import google_search

    __all__ = [
        "organization_search",
        "organization_detail",
        "async_organization_search",
        "async_organization_detail",
        "relation_search",
        "relation_detail",
        "relation_resolve",
        "async_relation_search",
        "async_relation_detail",
        "async_relation_resolve",
        "financial_kpi",
        "network",
        "related_persons",
        "profit_and_loss_account",
        "publications",
        "balance_sheet_accounts",
        "insolvency_publications",
        "annual_financial_statements",
        "news",
        "google_search",
        "async_google_search",
        "web_content",
        "async_web_content"
    ]
except ImportError as e:
    # Provide better error message for missing dependencies
    if "langchain" in str(e) or "langchain_core" in str(e):
        raise ImportError(
            "Could not import langchain package which is required for AI tools. "
            "Please install the required dependencies: "
            "pip install fusionbase[ai] "
            "or "
            "pip install langchain>=0.3.0 langchain-core>=0.3.0"
        ) from e
    __all__ = []


# Define get_registry function to avoid circular imports
def get_registry():
    """Get the tools registry without creating circular imports.

    Returns:
        The tools registry module
    """
    from . import registry
    return registry

__all__.append("get_registry")
