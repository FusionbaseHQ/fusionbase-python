"""Convenience relation tools for common Fusionbase relations.

These tools provide easy access to commonly used relations without needing to know
the specific relation IDs. They internally use the relation_resolve functionality
from the entity.relation module.
"""

from typing import Any, Dict

from typing_extensions import Annotated

# Try to import required packages, raise informative error if not available
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

from fusionbase import Fusionbase


@tool
def financial_kpi(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get financial KPIs for an organization.

    This tool provides an extensive overview of key financial metrics including:
    - Detailed employee statistics
    - Diverse revenue figures
    - Accurate profit and loss data
    - Total balance sums

    These metrics offer deep insights into an organization's financial health and operational efficiency.

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the financial_kpi relation (ID: 5601746763).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("5601746763")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "5601746763",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving financial KPIs: {str(e)}"}


@tool
def network(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get the network of connected entities for an organization.

    A network relation refers to the connected entities that are related to a given root entity,
    such as:
    - Subsidiaries
    - Parent companies
    - Partner companies
    - Other affiliated organizations

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the network relation (ID: 3138484719).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("3138484719")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "3138484719",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving network: {str(e)}"}


@tool
def related_persons(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get management positions and related persons for an organization.

    This includes current and former members of the executive team, such as:
    - Managing directors
    - Authorized representatives
    - Board members
    - Other senior employees responsible for the management and organization of the company

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the related_persons relation (ID: 5906082026).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("5906082026")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "5906082026",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving related persons: {str(e)}"}


@tool
def profit_and_loss_account(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get profit and loss account information for an organization.

    This overview provides a detailed account of a company's financial results, including:
    - Expenses breakdown
    - Revenue streams
    - Net outcomes

    Useful for analyzing profitability over a specific period.

    Note: Not all companies might have data available for this relation.
    This is a convenience tool that internally uses the relation_resolve functionality
    with the profit_and_loss_account relation (ID: 5601734199).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("5601734199")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "5601734199",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving profit and loss account: {str(e)}"}


@tool
def publications(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get publications from the German Handelsregister for an organization.

    Announcements from the German Handelsregister include details on:
    - Personnel changes (new appointments, resignations)
    - Seat relocations (changes in registered address)
    - New company names (rebranding, name changes)
    - Other official announcements for registered businesses

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the publications relation (ID: 2533389984).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("2533389984")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "2533389984",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving publications: {str(e)}"}


@tool
def balance_sheet_accounts(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get balance sheet accounts information for an organization.

    This overview provides a detailed breakdown of a company's balance sheet, covering:
    - Assets (current and non-current)
    - Liabilities (short-term and long-term)
    - Equity components

    All components are provided in depth for comprehensive financial analysis.

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the balance_sheet_accounts relation (ID: 5601746108).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("5601746108")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "5601746108",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving balance sheet accounts: {str(e)}"}


@tool
def insolvency_publications(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get insolvency publications for an organization.

    Real-time queries from insolvency courts providing:
    - Latest status of insolvency proceedings
    - Progress updates on corporate insolvency cases
    - Court decisions and announcements
    - Important dates and deadlines

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the insolvency_publications relation (ID: 4655345278).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("4655345278")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "4655345278",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving insolvency publications: {str(e)}"}


@tool
def annual_financial_statements(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get annual financial statements for an organization.

    Published annual financial statements and additional reports of companies in the Federal Gazette,
    including:
    - Complete annual reports
    - Financial statements
    - Management reports
    - Auditor reports (if available)

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the annual_financial_statements relation (ID: 6322183029).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("6322183029")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "6322183029",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving annual financial statements: {str(e)}"}


@tool
def news(
    fb_entity_id: Annotated[str, "The Fusionbase entity ID of the organization"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get news articles about an organization.

    News articles and reports published by news providers, including:
    - Newspapers and online media
    - Industry publications
    - Reports about companies, their market presence, and industry trends
    - Recent developments and announcements

    Note: This is a convenience tool that internally uses the relation_resolve functionality
    with the news relation (ID: 5162071547).
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("5162071547")
        result = relation.resolve(fb_entity_id, {})

        return {
            "relation_id": "5162071547",
            "entity_id": fb_entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {"error": f"Error retrieving news: {str(e)}"}
