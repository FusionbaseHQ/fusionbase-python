"""Person entity tools for Fusionbase AI."""

from typing import Any, Dict, List

from typing_extensions import Annotated

# Try to import required packages, raise informative error if not available
try:
    from langchain_core.tools import InjectedToolArg
    from langchain_core.tools import tool
except ImportError:
    raise ImportError("Could not import langchain package. "
                      "Please install the required dependencies: "
                      "pip install fusionbase[ai] "
                      "or "
                      "pip install langchain>=0.3.0 langchain-core>=0.3.0")

from fusionbase import Fusionbase


@tool
def person_search(
    query: Annotated[
        str,
        "The person name to search for (works best with exact person names)"],
    client: Annotated[Fusionbase,
                      InjectedToolArg] = None) -> List[Dict[str, Any]]:
    """Search for persons in the Fusionbase database by name.

    This tool works best with specific person names, not generic descriptive queries.

    Example queries that work well:
    - "John Smith"
    - "Angela Merkel"
    - "Elon Musk"
    - "Dr. Michael Schmidt"

    Example queries that will NOT work well:
    - "CEOs in Berlin"
    - "board members"
    - "executives"

    Use exact or partial person names for optimal results.
    """
    if not client:
        return [{"error": "Fusionbase client is required."}]

    try:
        # Execute the search with default limit
        results = client.search.persons.search(q=query)

        # Format results in a readable way for LLM
        formatted_results = []
        if hasattr(results, 'items'):
            for person_ref in results.items:
                try:
                    # Get basic information without loading full entity
                    person_info = {
                        "entity_id": person_ref.entity_id,
                        "given_name": getattr(person_ref, "given_name", None),
                        "family_name": getattr(person_ref, "family_name", None),
                        "score": getattr(person_ref, "score", None)
                    }
                    formatted_results.append(person_info)
                except Exception as e:
                    formatted_results.append({
                        "entity_id":
                            getattr(person_ref, "entity_id", "unknown"),
                        "error":
                            str(e)
                    })

        return formatted_results

    except Exception as e:
        return [{"error": f"Search failed: {str(e)}"}]


@tool
async def async_person_search(
    query: Annotated[
        str,
        "The person name to search for (works best with exact person names)"],
    client: Annotated[Fusionbase,
                      InjectedToolArg] = None) -> List[Dict[str, Any]]:
    """Asynchronously search for persons in the Fusionbase database by name.

    This tool works best with specific person names, not generic descriptive queries.

    Example queries that work well:
    - "John Smith"
    - "Angela Merkel"
    - "Elon Musk"
    - "Dr. Michael Schmidt"

    Example queries that will NOT work well:
    - "CEOs in Berlin"
    - "board members"
    - "executives"

    Use exact or partial person names for optimal results.
    """
    if not client:
        return [{"error": "Fusionbase client is required."}]

    # Execute the search with default limit
    results = await client.search.persons.asearch(q=query)

    # Format results in a readable way for LLM
    formatted_results = []
    for person_ref in results.items:
        try:
            # Get basic information without loading full entity
            person_info = {
                "entity_id": person_ref.entity_id,
                "given_name": getattr(person_ref, "given_name", None),
                "family_name": getattr(person_ref, "family_name", None),
                "score": getattr(person_ref, "score", None)
            }
            formatted_results.append(person_info)
        except Exception as e:
            formatted_results.append({
                "entity_id": person_ref.entity_id,
                "error": str(e)
            })

    return formatted_results


@tool
def person_detail(
        entity_id: Annotated[str,
                             "Entity ID of the person to retrieve details for"],
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get detailed information about a person using their entity ID.

    Use this tool after finding a person with person_search
    to get complete information about the person.

    Returns comprehensive details about the person including:
    - Full name components (given name, family name, aliases)
    - Birth date information
    - Associated locations (home, birth place, etc.)
    - Data sources
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        person = client.entities.persons.from_id(entity_id)

        # Format the person information
        result = {
            "entity_id": person.fb_entity_id,
            "given_name": person.given_name,
            "family_name": person.family_name,
        }

        # Add name details if available
        if hasattr(person, "name") and person.name:
            name_info = {}
            if person.name.given:
                name_info["given"] = person.name.given
            if person.name.family:
                name_info["family"] = person.name.family
            if person.name.maiden:
                name_info["maiden"] = person.name.maiden
            if person.name.aliases:
                name_info["aliases"] = person.name.aliases

            if name_info:
                result["name_details"] = name_info

        # Add birth date if available
        if hasattr(person, "birth_date") and person.birth_date:
            birth_info = {}
            if person.birth_date.value:
                birth_info["value"] = str(person.birth_date.value)
            if hasattr(person.birth_date, "is_month"):
                birth_info["is_month"] = person.birth_date.is_month

            if birth_info:
                result["birth_date"] = birth_info

        # Add locations if available
        if hasattr(person, "locations") and person.locations:
            locations_info = {}
            for location_type, location in person.locations.items():
                if location and hasattr(location, "formatted_address"):
                    locations_info[location_type] = {
                        "entity_id": getattr(location, "fb_entity_id", None),
                        "formatted_address": location.formatted_address,
                        "name": getattr(location, "name", None)
                    }

            if locations_info:
                result["locations"] = locations_info

        # Add source information if available
        if hasattr(person, "source") and person.source:
            result["source"] = {
                "name": getattr(person.source, "name", None),
                "key": getattr(person.source, "key", None)
            }

        # Add external IDs if available
        if hasattr(person, "external_ids") and person.external_ids:
            result["external_ids"] = person.external_ids

        return result

    except Exception as e:
        return {"error": f"Failed to retrieve person details: {str(e)}"}


@tool
async def async_person_detail(
        entity_id: Annotated[str,
                             "Entity ID of the person to retrieve details for"],
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get detailed information about a person using their entity ID asynchronously.

    Use this tool after finding a person with person_search
    to get complete information about the person.

    Returns comprehensive details about the person including:
    - Full name components (given name, family name, aliases)
    - Birth date information
    - Associated locations (home, birth place, etc.)
    - Data sources
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        person = await client.entities.persons.afrom_id(entity_id)

        # Format the person information
        result = {
            "entity_id": person.fb_entity_id,
            "given_name": person.given_name,
            "family_name": person.family_name,
        }

        # Add name details if available
        if hasattr(person, "name") and person.name:
            name_info = {}
            if person.name.given:
                name_info["given"] = person.name.given
            if person.name.family:
                name_info["family"] = person.name.family
            if person.name.maiden:
                name_info["maiden"] = person.name.maiden
            if person.name.aliases:
                name_info["aliases"] = person.name.aliases

            if name_info:
                result["name_details"] = name_info

        # Add birth date if available
        if hasattr(person, "birth_date") and person.birth_date:
            birth_info = {}
            if person.birth_date.value:
                birth_info["value"] = str(person.birth_date.value)
            if hasattr(person.birth_date, "is_month"):
                birth_info["is_month"] = person.birth_date.is_month

            if birth_info:
                result["birth_date"] = birth_info

        # Add locations if available
        if hasattr(person, "locations") and person.locations:
            locations_info = {}
            for location_type, location in person.locations.items():
                if location and hasattr(location, "formatted_address"):
                    locations_info[location_type] = {
                        "entity_id": getattr(location, "fb_entity_id", None),
                        "formatted_address": location.formatted_address,
                        "name": getattr(location, "name", None)
                    }

            if locations_info:
                result["locations"] = locations_info

        # Add source information if available
        if hasattr(person, "source") and person.source:
            result["source"] = {
                "name": getattr(person.source, "name", None),
                "key": getattr(person.source, "key", None)
            }

        # Add external IDs if available
        if hasattr(person, "external_ids") and person.external_ids:
            result["external_ids"] = person.external_ids

        return result

    except Exception as e:
        return {"error": f"Failed to retrieve person details: {str(e)}"}
