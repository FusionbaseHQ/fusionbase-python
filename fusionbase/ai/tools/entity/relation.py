"""Relation entity tools for Fusionbase AI.

Relations in Fusionbase represent connections between different entity types (like organizations,
people, locations). They define specific ways entities are related to each other, such as:

- Organization networks (parent-subsidiary relationships)
- Board memberships (person-to-organization relationships)
- Geographical relationships (organization-to-location relationships)

Relations can also connect entities to statistical indicators and features:
- Organizations to financial data (revenue, employee count, growth rates)
- Locations to economic indicators (GDP, unemployment rate, inflation)
- Locations to demographic statistics (population, death rates, birth rates)
- Organizations to industry-specific metrics (market share, production capacity)

Features in this context represent statistical data points or indicators that provide
quantitative information about entities. For example, resolving a "REVENUE" relation
with a company might return its annual revenue figures over time.

Working with relations involves two main operations:
1. Viewing relation details - Understanding what a relation represents and its parameters
2. Resolving a relation - Applying the relation to an entity to get related entities/data
"""

from typing import Any, Dict, List, Optional

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
        "pip install langchain>=0.3.0 langchain-core>=0.3.0") from exc

from fusionbase import Fusionbase
from fusionbase.search.relation_search import RelationSearchParams


@tool
def relation_search(
    query: Annotated[str, "The relation name or concept to search for"],
    limit: Annotated[int, "Maximum number of results to return"] = 5,
    client: Annotated[Fusionbase,
                      InjectedToolArg] = None) -> List[Dict[str, Any]]:
    """Search for relations (connection types) in the Fusionbase database.

    Relations define how different entities are connected to each other or to statistical data. For example:
    - A "NETWORK" relation connects parent and subsidiary companies
    - A "BOARD_MEMBER_OF" relation connects people to organizations
    - A "REVENUE" relation connects organizations to financial data indicators
    - A "POPULATION" relation connects locations to demographic statistics
    - A "GDP" relation connects locations to economic indicators

    Relations can provide both entity connections and statistical features/indicators:
    - Entity connections: Links between organizations, people, locations
    - Statistical indicators: Revenue, employee count, growth rates
    - Economic data: GDP, unemployment rates, market indicators
    - Demographic data: Population, birth rates, death rates

    Example queries:
    - "network" (finds parent-subsidiary company relationships)
    - "location" (finds geographical relationships)
    - "board member" (finds person-to-organization relationships)
    - "revenue" (finds financial indicators)
    - "population" (finds demographic indicators)

    Returns a list of relation types that match the search query.
    """
    if not client:
        return [{"error": "Fusionbase client is required."}]

    # Execute the search
    results = client.search.relations.search(
        RelationSearchParams(q=query, limit=limit))

    # Format results in a readable way for LLM
    formatted_results = []
    for relation_ref in results.items:
        try:
            # Get full relation details
            relation = relation_ref.get()

            # Extract parameter info if available
            parameters = []
            if relation.resolve_config and relation.resolve_config.parameter_definition:
                parameters = [
                    {
                        "name": param.name,
                        "type": param.type,
                        "required": param.required
                    }
                    for param in relation.resolve_config.parameter_definition
                    if param.name  # Only include params with names
                ]

            # Prepare relation summary
            relation_info = {
                "relation_id":
                    relation.relation_id,
                "name":
                    relation.relation_name,
                "label":
                    relation.label,
                "model_from":
                    relation.model_from.value if relation.model_from else None,
                "model_to":
                    relation.model_to.value if relation.model_to else None,
                "can_resolve":
                    bool(relation.resolve_config),
                "parameters":
                    parameters if parameters else None,
            }
            formatted_results.append(relation_info)
        except Exception as e:
            formatted_results.append({
                "relation_id": relation_ref.entity_id,
                "error": str(e)
            })

    return formatted_results


@tool
async def async_relation_search(
    query: Annotated[str, "The relation name or concept to search for"],
    limit: Annotated[int, "Maximum number of results to return"] = 5,
    client: Annotated[Fusionbase,
                      InjectedToolArg] = None) -> List[Dict[str, Any]]:
    """Asynchronously search for relations (connection types) in the Fusionbase database.

    Relations define how different entities are connected to each other or to statistical data. For example:
    - A "NETWORK" relation connects parent and subsidiary companies
    - A "BOARD_MEMBER_OF" relation connects people to organizations
    - A "REVENUE" relation connects organizations to financial data indicators
    - A "POPULATION" relation connects locations to demographic statistics
    - A "GDP" relation connects locations to economic indicators

    Relations can provide both entity connections and statistical features/indicators:
    - Entity connections: Links between organizations, people, locations
    - Statistical indicators: Revenue, employee count, growth rates
    - Economic data: GDP, unemployment rates, market indicators
    - Demographic data: Population, birth rates, death rates

    Example queries:
    - "network" (finds parent-subsidiary company relationships)
    - "location" (finds geographical relationships)
    - "board member" (finds person-to-organization relationships)
    - "revenue" (finds financial indicators)
    - "population" (finds demographic indicators)

    Returns a list of relation types that match the search query.
    """
    if not client:
        return [{"error": "Fusionbase client is required."}]

    # Execute the search
    results = await client.search.relations.asearch(
        RelationSearchParams(q=query, limit=limit))

    # Format results in a readable way for LLM
    formatted_results = []
    for relation_ref in results.items:
        try:
            # Get full relation details asynchronously
            relation = await relation_ref.aget()

            # Extract parameter info if available
            parameters = []
            if relation.resolve_config and relation.resolve_config.parameter_definition:
                parameters = [
                    {
                        "name": param.name,
                        "type": param.type,
                        "required": param.required
                    }
                    for param in relation.resolve_config.parameter_definition
                    if param.name  # Only include params with names
                ]

            # Prepare relation summary
            relation_info = {
                "relation_id":
                    relation.relation_id,
                "name":
                    relation.relation_name,
                "label":
                    relation.label,
                "model_from":
                    relation.model_from.value if relation.model_from else None,
                "model_to":
                    relation.model_to.value if relation.model_to else None,
                "can_resolve":
                    bool(relation.resolve_config),
                "parameters":
                    parameters if parameters else None,
            }
            formatted_results.append(relation_info)
        except Exception as e:
            formatted_results.append({
                "relation_id": relation_ref.entity_id,
                "error": str(e)
            })

    return formatted_results


@tool
def relation_detail(
        relation_id: Annotated[str,
                               "ID of the relation to retrieve details for"],
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get detailed information about a relation type using its ID.

    VIEWING a relation (this function) simply tells you WHAT the relation represents
    and HOW it can be used, without actually retrieving any connected entities or data.

    A relation defines a specific type of connection between entities or to statistical data:
    - The "NETWORK" relation connects parent companies to subsidiaries
    - The "BOARD_MEMBER_OF" relation connects people to organizations
    - The "REVENUE" relation connects an organization to its financial data
    - The "GDP" relation connects a location to economic indicators
    - The "POPULATION" relation connects a location to demographic statistics

    Relations with model_to=FEATURE typically provide statistical indicators or metrics
    about the entity in model_from. These feature relations deliver quantitative data
    rather than connections to other entities.

    This tool provides information about:
    - What entity types the relation connects (e.g., organization-to-organization or organization-to-feature)
    - Any parameters needed to resolve the relation (get actual related entities or data)
    - Description of what the relation represents or measures

    To actually get connected entities or statistical data, use the relation_resolve tool
    after understanding the relation with this tool.
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get relation by ID
        relation = client.entities.relations.from_id(relation_id)

        # Build a comprehensive result
        result = {
            "relation_id":
                relation.relation_id,
            "name":
                relation.relation_name,
            "label":
                relation.label,
            "description":
                relation.relation_description,
            "model_from":
                relation.model_from.value if relation.model_from else None,
            "model_to":
                relation.model_to.value if relation.model_to else None,
            "can_resolve":
                bool(relation.resolve_config),
        }

        # Add parameter information if available
        if relation.resolve_config and relation.resolve_config.parameter_definition:
            result["parameters"] = [
                {
                    "name":
                        param.name,
                    "type":
                        param.type,
                    "description":
                        param.description.en if param.description and
                        hasattr(param.description, "en") else None,
                    "required":
                        param.required,
                    "default":
                        param.default
                }
                for param in relation.resolve_config.parameter_definition
                if param.name  # Only include params with names
            ]

        # Add metadata if available
        if relation.metadata:
            result["created_at"] = relation.metadata.get("created_at")
            result["updated_at"] = relation.metadata.get("updated_at")

        return result
    except Exception as e:
        return {"error": f"Error retrieving relation: {str(e)}"}


@tool
async def async_relation_detail(
        relation_id: Annotated[str,
                               "ID of the relation to retrieve details for"],
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Asynchronously get detailed information about a relation type using its ID.

    VIEWING a relation (this function) simply tells you WHAT the relation represents
    and HOW it can be used, without actually retrieving any connected entities or data.

    A relation defines a specific type of connection between entities or to statistical data:
    - The "NETWORK" relation connects parent companies to subsidiaries
    - The "BOARD_MEMBER_OF" relation connects people to organizations
    - The "REVENUE" relation connects an organization to its financial data
    - The "GDP" relation connects a location to economic indicators
    - The "POPULATION" relation connects a location to demographic statistics

    Relations with model_to=FEATURE typically provide statistical indicators or metrics
    about the entity in model_from. These feature relations deliver quantitative data
    rather than connections to other entities.

    This tool provides information about:
    - What entity types the relation connects (e.g., organization-to-organization or organization-to-feature)
    - Any parameters needed to resolve the relation (get actual related entities or data)
    - Description of what the relation represents or measures

    To actually get connected entities or statistical data, use the relation_resolve tool
    after understanding the relation with this tool.
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # Get relation by ID asynchronously
        relation = await client.entities.relations.afrom_id(relation_id)

        # Build a comprehensive result
        result = {
            "relation_id":
                relation.relation_id,
            "name":
                relation.relation_name,
            "label":
                relation.label,
            "description":
                relation.relation_description,
            "model_from":
                relation.model_from.value if relation.model_from else None,
            "model_to":
                relation.model_to.value if relation.model_to else None,
            "can_resolve":
                bool(relation.resolve_config),
        }

        # Add parameter information if available
        if relation.resolve_config and relation.resolve_config.parameter_definition:
            result["parameters"] = [
                {
                    "name":
                        param.name,
                    "type":
                        param.type,
                    "description":
                        param.description.en if param.description and
                        hasattr(param.description, "en") else None,
                    "required":
                        param.required,
                    "default":
                        param.default
                }
                for param in relation.resolve_config.parameter_definition
                if param.name  # Only include params with names
            ]

        # Add metadata if available
        if relation.metadata:
            result["created_at"] = relation.metadata.get("created_at")
            result["updated_at"] = relation.metadata.get("updated_at")

        return result
    except Exception as e:
        return {"error": f"Error retrieving relation: {str(e)}"}


@tool
def relation_resolve(
        relation_id: Annotated[str, "ID of the relation to resolve"],
        entity_id: Annotated[str,
                             "ID of the entity to resolve the relation with"],
        parameters: Annotated[Optional[Dict[
            str, Any]], "Optional parameters for relation resolution"] = None,
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Get entities or statistical data connected through a specific relation type.

    RESOLVING a relation (this function) actually retrieves the connected entities or data,
    unlike relation_detail which only describes what the relation means.

    The data returned depends on the relation type:
    - Entity-to-entity relations: Returns connected entities (e.g., subsidiaries, board members)
    - Entity-to-feature relations: Returns statistical data (e.g., revenue figures, employee counts)
    - Location-to-feature relations: Returns indicators (e.g., GDP values, population statistics)

    IMPORTANT: The entity_id MUST match the relation's "model_from" entity type.
    For example:
    - A relation with model_from=ORGANIZATION requires an organization entity ID
    - A relation with model_from=PERSON requires a person entity ID
    - A relation with model_from=LOCATION requires a location entity ID

    You can't resolve a relation with an entity of the wrong type. Always check the
    relation's "model_from" field first using relation_detail before attempting to resolve.

    Examples of relation resolutions:
    - Resolving a "NETWORK" relation with a parent company returns its subsidiaries
    - Resolving a "BOARD_MEMBER_OF" relation with a person returns organizations they serve
    - Resolving a "REVENUE" relation with a company returns financial data over time
    - Resolving a "POPULATION" relation with a location returns demographic statistics

    The relation_id specifies WHICH connection type to use, and the entity_id specifies
    FOR WHICH entity to find connections or data. Some relations require additional parameters,
    which can be discovered by first using relation_detail.

    When to use:
    - Use AFTER understanding what the relation represents (via relation_detail)
    - Use WHEN you need to get actual connected entities or statistical data
    - Use ONLY WITH an entity ID that matches the relation's "model_from" type
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # First get the relation to verify it exists and for caching
        relation = client.entities.relations.from_id(relation_id)

        # Now resolve it with the entity
        result = relation.resolve(entity_id, parameters or {})

        return {
            "relation_id": relation_id,
            "entity_id": entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {
            "error": f"Error resolving relation: {str(e)}",
            "relation_id": relation_id,
            "entity_id": entity_id
        }


@tool
async def async_relation_resolve(
        relation_id: Annotated[str, "ID of the relation to resolve"],
        entity_id: Annotated[str,
                             "ID of the entity to resolve the relation with"],
        parameters: Annotated[Optional[Dict[
            str, Any]], "Optional parameters for relation resolution"] = None,
        client: Annotated[Fusionbase,
                          InjectedToolArg] = None) -> Dict[str, Any]:
    """Asynchronously get entities or statistical data connected through a specific relation type.

    RESOLVING a relation (this function) actually retrieves the connected entities or data,
    unlike relation_detail which only describes what the relation means.

    The data returned depends on the relation type:
    - Entity-to-entity relations: Returns connected entities (e.g., subsidiaries, board members)
    - Entity-to-feature relations: Returns statistical data (e.g., revenue figures, employee counts)
    - Location-to-feature relations: Returns indicators (e.g., GDP values, population statistics)

    IMPORTANT: The entity_id MUST match the relation's "model_from" entity type.
    For example:
    - A relation with model_from=ORGANIZATION requires an organization entity ID
    - A relation with model_from=PERSON requires a person entity ID
    - A relation with model_from=LOCATION requires a location entity ID

    You can't resolve a relation with an entity of the wrong type. Always check the
    relation's "model_from" field first using relation_detail before attempting to resolve.

    Examples of relation resolutions:
    - Resolving a "NETWORK" relation with a parent company returns its subsidiaries
    - Resolving a "BOARD_MEMBER_OF" relation with a person returns organizations they serve
    - Resolving a "REVENUE" relation with a company returns financial data over time
    - Resolving a "POPULATION" relation with a location returns demographic statistics

    The relation_id specifies WHICH connection type to use, and the entity_id specifies
    FOR WHICH entity to find connections or data. Some relations require additional parameters,
    which can be discovered by first using relation_detail.

    When to use:
    - Use AFTER understanding what the relation represents (via relation_detail)
    - Use WHEN you need to get actual connected entities or statistical data
    - Use ONLY WITH an entity ID that matches the relation's "model_from" type
    """
    if not client:
        return {"error": "Fusionbase client is required."}

    try:
        # First get the relation asynchronously to verify it exists and for caching
        relation = await client.entities.relations.afrom_id(relation_id)

        # Now resolve it with the entity asynchronously
        result = await relation.aresolve(entity_id, parameters or {})

        return {
            "relation_id": relation_id,
            "entity_id": entity_id,
            "relation_name": relation.relation_name,
            "result": result
        }
    except Exception as e:
        return {
            "error": f"Error resolving relation: {str(e)}",
            "relation_id": relation_id,
            "entity_id": entity_id
        }
