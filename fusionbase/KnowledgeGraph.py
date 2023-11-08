import json
from urllib.parse import urlencode, quote

class KnowledgeGraph:
    
    
    @staticmethod
    def get(client, intent, from_entity_type, from_entity_id, relation_id, relation_parameters):
        """
        Get knowledge graph data for a given intent and entity.
        
        Args:
        client (httpx.Client): The client to use for the request.
        intent (str): The intent to get knowledge graph data for.
        from_entity_type (str): The type of the entity to get knowledge graph data for.
        from_entity_id (str): The ID of the entity to get knowledge graph data for.
        relation_id (str): The ID of the relation to get knowledge graph data for.
        relation_parameters (dict): A dictionary of parameters to pass to the relation.
        
        Returns:
        dict: The knowledge graph data.
        """        

        # Your parameters
        params = {
            'intent': intent,
            'from_entity_type': from_entity_type,
            'from_entity_id': from_entity_id,
            'relation_id': relation_id,
            # Serialize and then URL-encode the nested dictionary for 'relation_parameters'
            'relation_parameters': json.dumps(relation_parameters.get("value")) if relation_parameters is not None else None
        }

        # Encode the parameters
        # We use 'quote' from 'urllib.parse' to ensure the JSON string is properly URL-encoded
        encoded_params = urlencode(params, quote_via=quote)

        # Construct the URL with the encoded parameters
        url = f'/search/knowledge-graph?{encoded_params}'

        # Construct the URL with the encoded parameters
        url = f'/search/knowledge-graph?{encoded_params}'
        
        result = client.get(url)
        return result.json()
        