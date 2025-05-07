"""DataService manager for Fusionbase SDK."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from fusionbase.data.dataservice import DataService
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError


class DataServiceManager:
    """Manager for accessing data services.

    This class provides a centralized way to work with data services.
    Follows the same pattern as EntityManager and SearchManager.
    """

    def __init__(self, client):
        """Initialize the manager with a client.

        Args:
            client: Fusionbase client
        """
        self._client = client
        self._services_cache = {}  # Cache for services

    def _normalize_service_id(self, service_id: str) -> str:
        """Normalize service ID to handle different formats.

        Args:
            service_id: The service identifier (can be with or without collection prefix)

        Returns:
            Normalized service key (without collection prefix)
        """
        # Handle different ID formats
        if service_id.startswith("services/"):
            return service_id[9:]  # Remove "services/" prefix
        if service_id.startswith("_id:"):
            id_value = service_id[4:]  # Remove "_id:" prefix
            # Extract key part if it has a collection prefix
            if '/' in id_value:
                return id_value.split('/')[-1]
            return id_value
        if service_id.startswith("id:"):
            id_value = service_id[3:]  # Remove "id:" prefix
            # Extract key part if it has a collection prefix
            if '/' in id_value:
                return id_value.split('/')[-1]
            return id_value

        # For plain IDs, extract key if it has collection prefix
        if '/' in service_id:
            return service_id.split('/')[-1]
        return service_id

    def get_service(self, service_id: str) -> DataService:
        """Get a data service by ID.

        Args:
            service_id: The service identifier (can be with or without collection prefix)

        Returns:
            DataService instance
        """
        normalized_id = self._normalize_service_id(service_id)
        if normalized_id not in self._services_cache:
            # Store the original service_id but use the normalized one internally
            self._services_cache[normalized_id] = DataService(
                self._client, service_id)
            # Ensure the service uses the normalized key
            self._services_cache[normalized_id]._service_key = normalized_id
        return self._services_cache[normalized_id]

    def from_id(self, service_id: str, validate: bool = True) -> DataService:
        """Get a data service by ID. Consistent with entity managers.

        Args:
            service_id: The service identifier
            validate: Whether to validate if the service exists (default: True)

        Returns:
            DataService instance

        Raises:
            ResourceNotFoundError: If validate=True and the service doesn't exist
        """
        normalized_id = self._normalize_service_id(service_id)
        if normalized_id not in self._services_cache:
            # Store the original service_id but use the normalized one internally
            self._services_cache[normalized_id] = DataService(
                self._client, service_id)
            # Ensure the service uses the normalized key
            self._services_cache[normalized_id]._service_key = normalized_id

        # Validate that the service exists by fetching its metadata
        if validate:
            try:
                self._services_cache[normalized_id].get_metadata()
            except APIError as e:
                # Remove from cache and re-raise as ResourceNotFoundError
                del self._services_cache[normalized_id]
                raise ResourceNotFoundError("data_service", service_id,
                                            getattr(e, "response", None)) from e

        return self._services_cache[normalized_id]

    def from_key(self, service_key: str, validate: bool = True) -> DataService:
        """Get a data service by key. Alias for from_id for backward compatibility.

        Args:
            service_key: The service identifier
            validate: Whether to validate if the service exists (default: True)

        Returns:
            DataService instance

        Raises:
            ResourceNotFoundError: If validate=True and the service doesn't exist
        """
        return self.from_id(service_key, validate=validate)

    async def afrom_id(self,
                       service_id: str,
                       validate: bool = True) -> DataService:
        """Asynchronously get a data service by ID.

        Args:
            service_id: The service identifier
            validate: Whether to validate if the service exists (default: True)

        Returns:
            DataService instance that has metadata pre-loaded asynchronously

        Raises:
            ResourceNotFoundError: If validate=True and the service doesn't exist
        """
        normalized_id = self._normalize_service_id(service_id)
        if normalized_id not in self._services_cache:
            # Create a new service instance
            service = DataService(self._client, service_id)
            # Ensure the service uses the normalized key
            service._service_key = normalized_id
            self._services_cache[normalized_id] = service
        else:
            service = self._services_cache[normalized_id]

        # Validate by pre-fetching metadata asynchronously if requested
        if validate:
            try:
                await service.aget_metadata()
            except APIError as e:
                # Remove from cache and re-raise as ResourceNotFoundError
                del self._services_cache[normalized_id]
                raise ResourceNotFoundError("data_service", service_id,
                                            getattr(e, "response", None)) from e

        return service

    async def afrom_key(self,
                        service_key: str,
                        validate: bool = True) -> DataService:
        """Asynchronously get a data service by key. Alias for afrom_id.

        Args:
            service_key: The service identifier
            validate: Whether to validate if the service exists (default: True)

        Returns:
            DataService instance that has metadata pre-loaded asynchronously

        Raises:
            ResourceNotFoundError: If validate=True and the service doesn't exist
        """
        return await self.afrom_id(service_key, validate=validate)

    def invoke(self,
               service_id: str,
               inputs: Dict[str, Any] = None,
               **kwargs) -> Any:
        """Invoke a data service with the provided inputs.

        Args:
            service_id: The service ID to invoke
            inputs: Dictionary of input parameters
            **kwargs: Additional input parameters

        Returns:
            Service response data
        """
        service = self.get_service(service_id)

        # Combine inputs and kwargs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        if kwargs:
            all_inputs.update(kwargs)

        return service.invoke(all_inputs)

    async def ainvoke(self,
                      service_id: str,
                      inputs: Dict[str, Any] = None,
                      **kwargs) -> Any:
        """Asynchronously invoke a data service with the provided inputs.

        Args:
            service_id: The service ID to invoke
            inputs: Dictionary of input parameters
            **kwargs: Additional input parameters

        Returns:
            Service response data
        """
        service = await self.afrom_id(service_id)

        # Combine inputs and kwargs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        if kwargs:
            all_inputs.update(kwargs)

        return await service.ainvoke(all_inputs)

    def batch_invoke_parallel(self,
                              service_id: str,
                              batch_inputs: List[Dict[str, Any]],
                              max_workers: int = None) -> List[Any]:
        """Invoke a data service with multiple sets of inputs in parallel using threads.

        Args:
            service_id: The service ID to invoke
            batch_inputs: List of input dictionaries, one per invocation
            max_workers: Maximum number of worker threads (defaults to min(32, os.cpu_count() + 4))

        Returns:
            List of service responses, in the same order as inputs
        """
        service = self.get_service(service_id)

        # Using ThreadPoolExecutor for parallel invocation
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a future for each input to track original order
            futures = []
            for i, inputs in enumerate(batch_inputs):
                # Store the index with the future to maintain original order
                future = executor.submit(service.invoke, inputs)
                futures.append((i, future))

            # Sort results by original index to maintain input order
            results = [None] * len(futures)
            for idx, future in futures:
                results[idx] = future.result()

            return results

    async def abatch_invoke_parallel(self,
                                     service_id: str,
                                     batch_inputs: List[Dict[str, Any]],
                                     max_concurrency: int = None) -> List[Any]:
        """Asynchronously invoke a data service with multiple sets of inputs in parallel.

        Args:
            service_id: The service ID to invoke
            batch_inputs: List of input dictionaries, one per invocation
            max_concurrency: Maximum number of concurrent tasks (defaults to no limit)

        Returns:
            List of service responses, in the same order as inputs
        """
        service = await self.afrom_id(service_id)

        # Create tasks for each input set
        tasks = [service.ainvoke(inputs) for inputs in batch_inputs]

        # If max_concurrency is specified, limit concurrent execution
        if max_concurrency and max_concurrency > 0:
            # Using semaphore to limit concurrency
            semaphore = asyncio.Semaphore(max_concurrency)

            async def limited_invoke(inputs):
                async with semaphore:
                    return await service.ainvoke(inputs)

            # Recreate tasks with limited concurrency
            tasks = [limited_invoke(inputs) for inputs in batch_inputs]

        # Run all tasks in parallel and gather results
        # Ensure that results are returned in the same order as the inputs
        results = await asyncio.gather(*tasks)
        return results
