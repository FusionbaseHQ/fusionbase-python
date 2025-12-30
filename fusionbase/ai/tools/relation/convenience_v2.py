"""Intent-aware convenience relation tools for Fusionbase.

These tools support both full data return and placeholder mode based on the LLM's intent.
The LLM decides per call whether it needs the full data for analysis or just a placeholder
for data enrichment/pass-through scenarios.
"""

from datetime import datetime
import hashlib
import json
from typing import Any, Dict, Literal, Optional

from typing_extensions import Annotated

try:
    from langchain_core.tools import InjectedToolArg
    from langchain_core.tools import tool
except ImportError as exc:
    raise ImportError("Could not import langchain package. "
                      "Please install the required dependencies: "
                      "pip install fusionbase[ai]") from exc

from fusionbase import Fusionbase

# Global storage for placeholder data (in production, this would be a proper cache)
_placeholder_store: Dict[str, Any] = {}


def _create_placeholder_id(tool_name: str, entity_id: str) -> str:
    """Create a unique placeholder ID."""
    timestamp = datetime.now().isoformat()
    unique_str = f"{tool_name}_{entity_id}_{timestamp}"
    # Create a shorter hash for cleaner placeholders
    hash_id = hashlib.md5(unique_str.encode()).hexdigest()[:8]
    return f"{tool_name}_{hash_id}"


def _store_placeholder_data(placeholder_id: str, data: Any) -> None:
    """Store data for a placeholder."""
    _placeholder_store[placeholder_id] = data


def get_placeholder_data(placeholder_id: str) -> Any:
    """Retrieve data for a placeholder."""
    return _placeholder_store.get(placeholder_id)


def clear_placeholder_store() -> None:
    """Clear all stored placeholder data."""
    _placeholder_store.clear()


@tool
def financial_kpi(
        fb_entity_id: Annotated[str,
                                "The Fusionbase entity ID of the organization"],
        return_mode:
    Annotated[
        Literal["full", "placeholder"],
        "How to return data: 'full' for complete data when you need to analyze/interpret it, "
        "'placeholder' for efficient pass-through when you just need to fetch and include the data"] = "full",
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get financial KPIs for an organization.

    This tool provides an extensive overview of key financial metrics including:
    - Detailed employee statistics
    - Diverse revenue figures
    - Accurate profit and loss data
    - Total balance sums

    Use return_mode='full' when you need to analyze or interpret the data.
    Use return_mode='placeholder' when you just need to fetch and pass the data through.
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("5601746763")
        result = relation.resolve(fb_entity_id, {})

        if return_mode == "placeholder":
            # Create placeholder and store data
            placeholder_id = _create_placeholder_id("financial_kpi",
                                                    fb_entity_id)
            full_data = {
                "relation_id": "5601746763",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
            _store_placeholder_data(placeholder_id, full_data)

            # Return summary with placeholder
            record_count = len(result) if isinstance(result, list) else 1
            return {
                "placeholder": f"<{placeholder_id}>",
                "summary": f"Financial KPIs retrieved: {record_count} records",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name
            }
        else:
            # Return full data for analysis
            return {
                "relation_id": "5601746763",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
    except Exception as e:
        return {"error": f"Error retrieving financial KPIs: {str(e)}"}


@tool
def balance_sheet_accounts(
        fb_entity_id: Annotated[str,
                                "The Fusionbase entity ID of the organization"],
        return_mode:
    Annotated[
        Literal["full", "placeholder"],
        "How to return data: 'full' for complete data when you need to analyze/interpret it, "
        "'placeholder' for efficient pass-through when you just need to fetch and include the data"] = "full",
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get balance sheet accounts for an organization.

    The balance sheet provides a snapshot of the company's financial position including:
    - Assets (current and non-current)
    - Liabilities (current and long-term)
    - Shareholders' equity

    Use return_mode='full' when you need to analyze the balance sheet.
    Use return_mode='placeholder' when you just need to fetch and include the data.
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("3867665248")
        result = relation.resolve(fb_entity_id, {})

        if return_mode == "placeholder":
            # Create placeholder and store data
            placeholder_id = _create_placeholder_id("balance_sheet",
                                                    fb_entity_id)
            full_data = {
                "relation_id": "3867665248",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
            _store_placeholder_data(placeholder_id, full_data)

            # Return summary with placeholder
            record_count = len(result) if isinstance(result, list) else 1
            return {
                "placeholder": f"<{placeholder_id}>",
                "summary": f"Balance sheet retrieved: {record_count} accounts",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name
            }
        else:
            # Return full data for analysis
            return {
                "relation_id": "3867665248",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
    except Exception as e:
        return {"error": f"Error retrieving balance sheet: {str(e)}"}


@tool
def profit_and_loss_account(
        fb_entity_id: Annotated[str,
                                "The Fusionbase entity ID of the organization"],
        return_mode:
    Annotated[
        Literal["full", "placeholder"],
        "How to return data: 'full' for complete data when you need to analyze/interpret it, "
        "'placeholder' for efficient pass-through when you just need to fetch and include the data"] = "full",
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get profit and loss statement for an organization.

    The P&L statement shows the company's revenues, costs, and expenses during a period:
    - Revenue/Sales
    - Cost of goods sold
    - Operating expenses
    - Net income

    Use return_mode='full' when you need to analyze profitability trends.
    Use return_mode='placeholder' when you just need to fetch and include the data.
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("4091408618")
        result = relation.resolve(fb_entity_id, {})

        if return_mode == "placeholder":
            # Create placeholder and store data
            placeholder_id = _create_placeholder_id("profit_loss", fb_entity_id)
            full_data = {
                "relation_id": "4091408618",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
            _store_placeholder_data(placeholder_id, full_data)

            # Return summary with placeholder
            record_count = len(result) if isinstance(result, list) else 1
            return {
                "placeholder":
                    f"<{placeholder_id}>",
                "summary":
                    f"P&L statement retrieved: {record_count} line items",
                "entity_id":
                    fb_entity_id,
                "relation_name":
                    relation.relation_name
            }
        else:
            # Return full data for analysis
            return {
                "relation_id": "4091408618",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
    except Exception as e:
        return {"error": f"Error retrieving P&L statement: {str(e)}"}


@tool
def annual_financial_statements(
        fb_entity_id: Annotated[str,
                                "The Fusionbase entity ID of the organization"],
        return_mode:
    Annotated[
        Literal["full", "placeholder"],
        "How to return data: 'full' for complete data when you need to analyze/interpret it, "
        "'placeholder' for efficient pass-through when you just need to fetch and include the data"] = "full",
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get annual financial statements/reports for an organization.

    Comprehensive annual reports including:
    - Complete financial statements
    - Management discussion and analysis
    - Auditor's reports
    - Notes to financial statements

    Use return_mode='full' when you need to analyze the reports.
    Use return_mode='placeholder' when you just need to fetch and include the data.
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get the relation and resolve it
        relation = client.entities.relations.from_id("890549372")
        result = relation.resolve(fb_entity_id, {})

        if return_mode == "placeholder":
            # Create placeholder and store data
            placeholder_id = _create_placeholder_id("annual_statements",
                                                    fb_entity_id)
            full_data = {
                "relation_id": "890549372",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
            _store_placeholder_data(placeholder_id, full_data)

            # Return summary with placeholder
            record_count = len(result) if isinstance(result, list) else 1
            return {
                "placeholder":
                    f"<{placeholder_id}>",
                "summary":
                    f"Annual statements retrieved: {record_count} documents",
                "entity_id":
                    fb_entity_id,
                "relation_name":
                    relation.relation_name
            }
        else:
            # Return full data for analysis
            return {
                "relation_id": "890549372",
                "entity_id": fb_entity_id,
                "relation_name": relation.relation_name,
                "result": result
            }
    except Exception as e:
        return {"error": f"Error retrieving annual statements: {str(e)}"}


# Export the placeholder retrieval function for the agent to use
__all__ = [
    'financial_kpi', 'balance_sheet_accounts', 'profit_and_loss_account',
    'annual_financial_statements', 'get_placeholder_data',
    'clear_placeholder_store'
]
