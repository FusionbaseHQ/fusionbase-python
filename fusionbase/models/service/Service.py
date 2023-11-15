from pydantic import BaseModel
from typing import List

from typing import Optional
import httpx

class InputDefinition(BaseModel):
    name: str
    type: str
    description: dict
    definition: dict
    sample: dict
    required: bool

class Service(BaseModel):
    key: str
    unique_label: str | None = None
    # Make source of type source and resolve
    source: dict | None = None
    request_result: dict | None = None
    service_input_definition: List[InputDefinition] | None = None
    
    client: Optional[httpx.Client] = None
    
    class Config:
        arbitrary_types_allowed = True
        
    
    def invoke(self, **kwargs) -> dict:
        try:
            # Construct the payload
            payload = {"key": self.key, "inputs": kwargs}
            # Attempt to post the request to the server
            response = self.client.post('service/invoke', timeout=60.0, json=payload)
            # Check if the response was successful
            response.raise_for_status()
            # Return the JSON response
            return response.json()

        except httpx.HTTPStatusError as http_err:
            # Handle HTTP errors (like 404, 503, etc.)
            print(f"HTTP error occurred: {http_err}")
            return {"error": "HTTP error occurred"}

        except httpx.NetworkError as net_err:
            # Handle network-related errors
            print(f"Network error occurred: {net_err}")
            return {"error": "Network error occurred"}

        except httpx.TimeoutException as timeout_err:
            # Handle timeout errors
            print(f"Timeout error occurred: {timeout_err}")
            return {"error": "Timeout error occurred"}

        except httpx.RequestError as req_err:
            # Handle any other request-related errors
            print(f"Request error occurred: {req_err}")
            return {"error": "Request error occurred"}

        except Exception as e:
            # Handle other exceptions
            print(f"An unexpected error occurred: {e}")
            return {"error": "Unexpected error occurred"}

