"""Organization entity tools for Fusionbase AI."""

from typing import Any, Dict, List

from typing_extensions import Annotated

# Try to import required packages, raise informative error if not available
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

from fusionbase import Fusionbase


@tool
def organization_search(
    query: Annotated[str, "The company name to search for (works best with exact company names)"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> List[Dict[str, Any]]:
    """Search for organizations in the Fusionbase database by name.

    This tool works best with specific company names, not generic descriptive queries.

    Example queries that work well:
    - "Microsoft Corporation"
    - "BMW AG"
    - "Apple Inc"

    Example queries that will NOT work well:
    - "startups in Berlin"
    - "renewable energy companies"
    - "tech startups"

    Use exact or partial company names for optimal results.
    """
    if not client:
        return [{"error": "Fusionbase client is required."}]

    try:
        # Execute the search with default limit
        results = client.search.organizations.search(q=query)

        # Format results in a readable way for LLM
        formatted_results = []
        if hasattr(results, 'items'):
            for org_ref in results.items:
                try:
                    # Get basic information without loading full entity
                    org_info = {
                        "entity_id": org_ref.entity_id,
                        "name": getattr(org_ref, "name", None),
                        "country": getattr(org_ref, "country", None),
                        "score": getattr(org_ref, "score", None)
                    }
                    formatted_results.append(org_info)
                except Exception as e:
                    formatted_results.append({
                        "entity_id": getattr(org_ref, "entity_id", "unknown"),
                        "error": str(e)
                    })

        return formatted_results

    except Exception as e:
        return [{"error": f"Search failed: {str(e)}"}]


@tool
async def async_organization_search(
    query: Annotated[str, "The company name to search for (works best with exact company names)"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> List[Dict[str, Any]]:
    """Asynchronously search for organizations in the Fusionbase database by name.

    This tool works best with specific company names, not generic descriptive queries.

    Example queries that work well:
    - "Microsoft Corporation"
    - "BMW AG"
    - "Apple Inc"

    Example queries that will NOT work well:
    - "startups in Berlin"
    - "renewable energy companies"
    - "tech startups"

    Use exact or partial company names for optimal results.
    """
    if not client:
        return [{"error": "Fusionbase client is required."}]

    # Execute the search with default limit
    results = await client.search.organizations.asearch(q=query)

    # Format results in a readable way for LLM
    formatted_results = []
    for org_ref in results.items:
        try:
            # Get basic information without loading full entity
            org_info = {
                "entity_id": org_ref.entity_id,
                "name": getattr(org_ref, "name", None),
                "country": getattr(org_ref, "country", None),
                "score": getattr(org_ref, "score", None)
            }
            formatted_results.append(org_info)
        except Exception as e:
            formatted_results.append({
                "entity_id": org_ref.entity_id,
                "error": str(e)
            })

    return formatted_results


@tool
def organization_detail(
    entity_id: Annotated[str, "Entity ID of the organization to retrieve details for"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get detailed information about an organization using its entity ID.

    Use this tool after finding an organization with fusionbase_organization_search
    to get complete information about the organization.

    Returns comprehensive details about the organization including:
    - Full name
    - Website
    - Industry
    - Address
    - Contact information
    - External identifiers
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    org = client.entities.organizations.from_id(entity_id)

    # Format the organization information
    result = {
        "entity_id": org.fb_entity_id,
        "name": org.name,
        "primary_website": getattr(org, "primary_website", None),
        "country": getattr(org, "country", None),
    }

    # Add address if available
    if hasattr(org, "address") and org.address:
        result["address"] = org.address.formatted_address

    # Add industry classifications if available
    if (hasattr(org, "classifications") and org.classifications and
        hasattr(org.classifications, "web") and org.classifications.web):
        result["industries"] = [
            {"source": c.source, "value": c.value.de}
            for c in org.classifications.web
            if hasattr(c, "value") and hasattr(c.value, "de")
        ]

    # Add founding date if available
    if hasattr(org, "founding_date") and org.founding_date:
        result["founding_date"] = str(org.founding_date)

    # Add contact information if available
    contact_info = {}
    if hasattr(org, "contact") and org.contact:
        # Add websites
        if org.contact.websites and org.contact.websites.primary:
            contact_info["website"] = org.contact.websites.primary

        # Add phone numbers
        if org.contact.phone_numbers and org.contact.phone_numbers.primary:
            contact_info["phone"] = org.contact.phone_numbers.primary

        # Add emails
        if org.contact.emails and org.contact.emails.primary:
            contact_info["email"] = org.contact.emails.primary

    if contact_info:
        result["contact"] = contact_info

    # Add external IDs
    if org.external_ids:
        result["external_ids"] = org.external_ids

    return result


@tool
async def async_organization_detail(
    entity_id: Annotated[str, "Entity ID of the organization to retrieve details for"],
    client: Annotated[Fusionbase, InjectedToolArg] = None
) -> Dict[str, Any]:
    """Get detailed information about an organization using its entity ID asynchronously.

    Use this tool after finding an organization with fusionbase_organization_search
    to get complete information about the organization.

    Returns comprehensive details about the organization including:
    - Full name
    - Website
    - Industry
    - Address
    - Contact information
    - External identifiers
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    org = await client.entities.organizations.afrom_id(entity_id)

    # Format the organization information
    result = {
        "entity_id": org.fb_entity_id,
        "name": org.name,
        "primary_website": getattr(org, "primary_website", None),
        "country": getattr(org, "country", None),
    }

    # Add address if available
    if hasattr(org, "address") and org.address:
        result["address"] = org.address.formatted_address

    # Add industry classifications if available
    if (hasattr(org, "classifications") and org.classifications and
        hasattr(org.classifications, "web") and org.classifications.web):
        result["industries"] = [
            {"source": c.source, "value": c.value.de}
            for c in org.classifications.web
            if hasattr(c, "value") and hasattr(c.value, "de")
        ]

    # Add founding date if available
    if hasattr(org, "founding_date") and org.founding_date:
        result["founding_date"] = str(org.founding_date)

    # Add contact information if available
    contact_info = {}
    if hasattr(org, "contact") and org.contact:
        # Add websites
        if org.contact.websites and org.contact.websites.primary:
            contact_info["website"] = org.contact.websites.primary

        # Add phone numbers
        if org.contact.phone_numbers and org.contact.phone_numbers.primary:
            contact_info["phone"] = org.contact.phone_numbers.primary

        # Add emails
        if org.contact.emails and org.contact.emails.primary:
            contact_info["email"] = org.contact.emails.primary

    if contact_info:
        result["contact"] = contact_info

    # Add external IDs
    if org.external_ids:
        result["external_ids"] = org.external_ids

    return result
