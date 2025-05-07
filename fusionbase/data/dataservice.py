"""DataService module for Fusionbase SDK."""

import asyncio
from datetime import datetime
import inspect
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from fusionbase.data.source import Source
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ValidationError


class LocalizedText(BaseModel):
    """Text with language codes."""
    model_config = ConfigDict(extra="allow")
    en: Optional[str] = None
    de: Optional[str] = None


class ServiceInputSample(BaseModel):
    """Sample value for a service input parameter."""
    value: Any


class ServiceInputDefinition(BaseModel):
    """Definition of an input parameter for a data service.

    Attributes:
        name: Name of the input parameter
        type: Data type (string, integer, etc.)
        description: Localized description
        definition: Additional definition details
        sample: Sample value for the parameter
        required: Whether this parameter is required
    """
    name: str
    type: str
    description: LocalizedText
    definition: Optional[LocalizedText] = None
    sample: Optional[ServiceInputSample] = None
    required: bool = False

    def validate_value(self, value: Any) -> Any:
        """Validate and convert a parameter value according to its type.

        Args:
            value: Value to validate

        Returns:
            Validated and converted value

        Raises:
            ValidationError: If validation fails
        """
        # Handle None case
        if value is None:
            if self.required:
                raise ValidationError(
                    f"Required parameter '{self.name}' is missing")
            return None

        # Try to convert value to expected type
        try:
            if self.type in ["string", "str"]:
                return str(value)
            elif self.type in ["integer", "int"]:
                return int(value)
            elif self.type in ["float", "number", "double"]:
                return float(value)
            elif self.type in ["boolean", "bool"]:
                if isinstance(value, str):
                    return value.lower() in ("yes", "true", "t", "1", "y")
                return bool(value)
            else:
                # For complex types, return as-is
                return value
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid value for parameter '{self.name}': "
                f"expected {self.type}, got {type(value).__name__}")


class CreditPolicy(BaseModel):
    """Credit usage policy for the service.

    Attributes:
        entity_action: The action that consumes credits
        credit_cost: Number of credits consumed per invocation
    """
    entity_action: str
    credit_cost: int


class DataServiceMetadata(BaseModel):
    """Metadata for a data service.

    Attributes:
        name: Localized name of the service
        description: Localized description
        updated_at: Last update timestamp
        source: Source information
        service_input_definition: Definition of required inputs
        key: Service key identifier (without collection prefix)
        id: Service identifier (may include collection prefix)
        credit_policy: Credit usage information
    """
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,  # Allow population by field name (without alias)
    )

    name: LocalizedText
    description: LocalizedText
    updated_at: datetime
    source: Source
    service_input_definition: List[ServiceInputDefinition]
    key: Optional[str] = Field(None,
                               description="Service identifier (key format)",
                               alias="_key")
    id: Optional[str] = Field(
        None,
        description="Service identifier (id format with collection prefix)")
    credit_policy: CreditPolicy

    @property
    def service_key(self) -> str:
        """Get the service key, preferring key but falling back to id."""
        if self.key:
            return self.key
        # Extract key from id if it's in format "services/12345"
        if self.id and '/' in self.id:
            return self.id.split('/')[-1]
        # Use id as-is if it doesn't have a prefix
        if self.id:
            return self.id
        return ""

    @property
    def service_id(self) -> str:
        """Get the full service ID (with collection prefix if available)."""
        if self.id:
            return self.id
        if self.key:
            # If we only have a key but need a full ID, construct it
            # Only do this if it doesn't already have the prefix
            if not self.key.startswith("services/"):
                return f"services/{self.key}"
            return self.key
        return ""

    @property
    def display_name(self) -> str:
        """Get a displayable name for the service."""
        if self.name.en:
            return self.name.en
        if self.name.de:
            return self.name.de
        return self.service_key

    @property
    def cost(self) -> int:
        """Get the credit cost for this service."""
        return self.credit_policy.credit_cost

    def get_parameter_definition(
            self, param_name: str) -> Optional[ServiceInputDefinition]:
        """Get parameter definition by name."""
        for param_def in self.service_input_definition:
            if param_def.name == param_name:
                return param_def
        return None


class DataService:
    """Client for interacting with Fusionbase data services.

    This class provides methods to fetch service metadata and invoke
    data services with the appropriate input parameters.

    Example:
        ```python
        # Get weather service and invoke it
        weather = client.services.from_id("1234567890")
        result = weather.invoke({"location": "Munich", "days": 3})

        # Or invoke directly with kwargs
        result = weather.invoke(location="Munich", days=3)

        # Use async version
        result = await weather.ainvoke(location="Munich", days=3)
        ```
    """

    def __init__(self, client, service_id: str = None):
        """Initialize a data service client.

        Args:
            client: Fusionbase client for API requests
            service_id: Optional service ID or key to initialize with
        """
        self._client = client
        # Store the original format provided by the user
        self._service_id = service_id
        # Extract just the key part if it's a full ID
        if service_id and '/' in service_id:
            self._service_key = service_id.split('/')[-1]
        else:
            self._service_key = service_id
        self._metadata = None

    @property
    def service_id(self) -> Optional[str]:
        """Get the service ID as originally provided."""
        return self._service_id

    @property
    def service_key(self) -> Optional[str]:
        """Get the service key (without collection prefix)."""
        return self._service_key

    @service_id.setter
    def service_id(self, value: str):
        """Set the service ID."""
        if value != self._service_id:
            self._service_id = value
            # Extract just the key part if it's a full ID
            if value and '/' in value:
                self._service_key = value.split('/')[-1]
            else:
                self._service_key = value
            self._metadata = None  # Reset metadata when service changes

    @service_key.setter
    def service_key(self, value: str):
        """Set the service key."""
        if value != self._service_key:
            self._service_key = value
            # Update service_id to match (without prefix)
            self._service_id = value
            self._metadata = None  # Reset metadata when service changes

    def get_metadata(self) -> DataServiceMetadata:
        """Fetch metadata for the current service.

        Returns:
            Service metadata

        Raises:
            APIError: If the service doesn't exist or an API error occurs
            ValueError: If no service key is set
        """
        if not self._service_key:
            raise ValueError(
                "No service key set. Set service_key or specify in constructor."
            )

        if self._metadata is None:
            try:
                # Always use just the key part for API requests
                data = self._client.request("GET",
                                            f"service/get/{self._service_key}")
                self._metadata = DataServiceMetadata.model_validate(data)
            except Exception as e:
                raise APIError(f"Failed to fetch service metadata: {e}",
                               status_code=500) from e

        return self._metadata

    async def aget_metadata(self) -> DataServiceMetadata:
        """Asynchronously fetch metadata for the current service.

        Returns:
            Service metadata

        Raises:
            APIError: If the service doesn't exist or an API error occurs
            ValueError: If no service key is set
        """
        if not self._service_key:
            raise ValueError(
                "No service key set. Set service_key or specify in constructor."
            )

        if self._metadata is None:
            try:
                # Try different async methods based on client capabilities
                if hasattr(self._client, "arequest"):
                    data = await self._client.arequest(
                        "GET", f"service/get/{self._service_key}")
                elif hasattr(self._client, "aget"):
                    data = await self._client.aget(
                        f"service/get/{self._service_key}")
                elif hasattr(self._client, "_async_http_client"):
                    response = await self._client._async_http_client.get(
                        f"service/get/{self._service_key}")
                    response.raise_for_status()
                    data = response.json()
                else:
                    # Fallback to sync method through asyncio.to_thread
                    if hasattr(self._client,
                               "request") and not inspect.iscoroutinefunction(
                                   self._client.request):
                        # Use to_thread for synchronous clients
                        metadata = await asyncio.to_thread(self.get_metadata)
                        return metadata
                    else:
                        # No suitable async method found
                        raise APIError(
                            "No suitable async method found to fetch service metadata",
                            status_code=500)

                # Convert data to DataServiceMetadata
                self._metadata = DataServiceMetadata.model_validate(data)
            except Exception as e:
                raise APIError(f"Failed to fetch service metadata: {e}",
                               status_code=500) from e

        return self._metadata

    def _validate_inputs(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the inputs against the service's input definition.

        Args:
            inputs: Dictionary of input parameters

        Returns:
            Validated input dictionary with type conversions applied

        Raises:
            ValidationError: If required parameters are missing or invalid
        """
        metadata = self.get_metadata()
        validated_inputs = {}

        # Check for required parameters and apply validations
        for param_def in metadata.service_input_definition:
            name = param_def.name
            value = inputs.get(name)

            # Validate the parameter
            validated_value = param_def.validate_value(value)

            # Only add non-None values to keep the request payload clean
            if validated_value is not None:
                validated_inputs[name] = validated_value

        # Check if any inputs weren't in the parameter definition
        for name, value in inputs.items():
            if name not in validated_inputs and metadata.get_parameter_definition(
                    name) is None:
                # Add it as-is since it's not defined in the metadata
                validated_inputs[name] = value

        return validated_inputs

    def invoke(self, inputs: Dict[str, Any] = None, **kwargs) -> Any:
        """Invoke the data service with the provided inputs.

        Args:
            inputs: Dictionary of input parameters
            **kwargs: Additional input parameters as keyword arguments

        Returns:
            Service response data

        Raises:
            ValidationError: If inputs are invalid
            APIError: If the service invocation fails
            ValueError: If no service key is set

        Example:
            ```python
            # Invoke with dictionary input
            result = service.invoke({"location": "Munich", "days": 3})

            # Or with kwargs
            result = service.invoke(location="Munich", days=3)
            ```
        """
        if not self._service_key:
            raise ValueError(
                "No service key set. Set service_key or specify in constructor."
            )

        # Combine inputs and kwargs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        if kwargs:
            all_inputs.update(kwargs)

        # Validate inputs against service definition
        validated_inputs = self._validate_inputs(all_inputs)

        # Prepare request payload
        payload = {"service_key": self._service_key, "inputs": validated_inputs}

        # Make request to service endpoint
        return self._client.request("POST", "service/invoke", json=payload)

    async def ainvoke(self, inputs: Dict[str, Any] = None, **kwargs) -> Any:
        """Asynchronously invoke the data service with the provided inputs.

        Args:
            inputs: Dictionary of input parameters
            **kwargs: Additional input parameters as keyword arguments

        Returns:
            Service response data

        Raises:
            ValidationError: If inputs are invalid
            APIError: If the service invocation fails
            ValueError: If no service key is set

        Example:
            ```python
            # Invoke asynchronously with dictionary input
            result = await service.ainvoke({"location": "Munich", "days": 3})

            # Or with kwargs
            result = await service.ainvoke(location="Munich", days=3)
            ```
        """
        if not self._service_key:
            raise ValueError(
                "No service key set. Set service_key or specify in constructor."
            )

        # Combine inputs and kwargs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        if kwargs:
            all_inputs.update(kwargs)

        # For async, we need to get metadata asynchronously if it's not already loaded
        if self._metadata is None:
            await self.aget_metadata()

        # Validate inputs against service definition
        # This needs to happen after we have metadata
        try:
            validated_inputs = self._validate_inputs(all_inputs)
        except ValidationError as e:
            # Make sure we propagate validation errors
            raise e

        # Prepare request payload
        payload = {"service_key": self._service_key, "inputs": validated_inputs}

        # Try different async methods depending on client capabilities
        if hasattr(self._client, "arequest"):
            return await self._client.arequest("POST",
                                               "service/invoke",
                                               json=payload)
        elif hasattr(self._client, "apost"):
            return await self._client.apost("service/invoke", json=payload)
        elif hasattr(self._client, "_async_http_client"):
            response = await self._client._async_http_client.post(
                "service/invoke", json=payload)
            response.raise_for_status()
            return response.json()
        else:
            # Fallback to sync method through asyncio.to_thread
            if hasattr(self._client,
                       "request") and not inspect.iscoroutinefunction(
                           self._client.request):
                return await asyncio.to_thread(self.invoke, inputs, **kwargs)

            # No suitable async method found
            raise APIError(
                "No suitable async method found for async service invocation",
                status_code=500)
