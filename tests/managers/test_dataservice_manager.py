"""Tests for DataServiceManager class."""

import asyncio
import unittest
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase.data.dataservice import DataService
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.managers.dataservice_manager import DataServiceManager


class TestDataServiceManagerInitialization(unittest.TestCase):
    """Test cases for DataServiceManager initialization."""

    def test_manager_creation(self):
        """Test creating DataServiceManager instance."""
        mock_client = MagicMock()

        manager = DataServiceManager(mock_client)

        self.assertEqual(manager._client, mock_client)
        self.assertEqual(manager._services_cache, {})


class TestDataServiceManagerNormalizeServiceId(unittest.TestCase):
    """Test cases for _normalize_service_id method."""

    def test_normalize_plain_id(self):
        """Test normalizing plain ID."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        result = manager._normalize_service_id("my_service")

        self.assertEqual(result, "my_service")

    def test_normalize_services_prefix(self):
        """Test normalizing ID with services/ prefix."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        result = manager._normalize_service_id("services/my_service")

        self.assertEqual(result, "my_service")

    def test_normalize_underscore_id_prefix(self):
        """Test normalizing ID with _id: prefix."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        result = manager._normalize_service_id("_id:my_service")

        self.assertEqual(result, "my_service")

    def test_normalize_underscore_id_with_collection(self):
        """Test normalizing _id: prefix with collection."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        result = manager._normalize_service_id("_id:services/my_service")

        self.assertEqual(result, "my_service")

    def test_normalize_id_prefix(self):
        """Test normalizing ID with id: prefix."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        result = manager._normalize_service_id("id:my_service")

        self.assertEqual(result, "my_service")

    def test_normalize_id_with_collection_prefix(self):
        """Test normalizing ID with collection/key format."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        result = manager._normalize_service_id("collection/my_service")

        self.assertEqual(result, "my_service")


class TestDataServiceManagerGetService(unittest.TestCase):
    """Test cases for get_service method."""

    def test_get_service_creates_dataservice(self):
        """Test get_service creates DataService instance."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            MockDataService.return_value = mock_service

            result = manager.get_service("test_service")

            MockDataService.assert_called_once_with(mock_client, "test_service")
            self.assertEqual(result, mock_service)

    def test_get_service_caches_service(self):
        """Test get_service caches the service instance."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            MockDataService.return_value = mock_service

            service1 = manager.get_service("test_service")
            service2 = manager.get_service("test_service")

            # Should only create once
            MockDataService.assert_called_once()
            self.assertIs(service1, service2)

    def test_get_service_normalizes_id(self):
        """Test get_service normalizes service ID."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            MockDataService.return_value = mock_service

            # Both should resolve to the same cached service
            manager.get_service("services/my_service")
            manager.get_service("my_service")

            # Should only create once due to normalization
            self.assertEqual(MockDataService.call_count, 1)


class TestDataServiceManagerFromId(unittest.TestCase):
    """Test cases for from_id method."""

    def test_from_id_validates_by_default(self):
        """Test from_id validates service existence by default."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.get_metadata.return_value = {"name": "test"}
            MockDataService.return_value = mock_service

            result = manager.from_id("test_service")

            mock_service.get_metadata.assert_called_once()
            self.assertEqual(result, mock_service)

    def test_from_id_skips_validation_when_disabled(self):
        """Test from_id skips validation when validate=False."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            MockDataService.return_value = mock_service

            result = manager.from_id("test_service", validate=False)

            mock_service.get_metadata.assert_not_called()
            self.assertEqual(result, mock_service)

    def test_from_id_raises_resource_not_found_on_api_error(self):
        """Test from_id raises ResourceNotFoundError when service not found."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.get_metadata.side_effect = APIError("Not found")
            MockDataService.return_value = mock_service

            with self.assertRaises(ResourceNotFoundError) as context:
                manager.from_id("missing_service")

            self.assertEqual(context.exception.resource_type, "data_service")

    def test_from_id_removes_from_cache_on_error(self):
        """Test from_id removes service from cache on validation error."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.get_metadata.side_effect = APIError("Not found")
            MockDataService.return_value = mock_service

            try:
                manager.from_id("failing_service")
            except ResourceNotFoundError:
                pass

            # Cache should not contain the failed service
            self.assertNotIn("failing_service", manager._services_cache)


class TestDataServiceManagerFromKey(unittest.TestCase):
    """Test cases for from_key method."""

    def test_from_key_is_alias_for_from_id(self):
        """Test from_key calls from_id."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.get_metadata.return_value = {}
            MockDataService.return_value = mock_service

            result = manager.from_key("test_key")

            self.assertEqual(result, mock_service)

    def test_from_key_passes_validate_parameter(self):
        """Test from_key passes validate parameter."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            MockDataService.return_value = mock_service

            manager.from_key("test_key", validate=False)

            mock_service.get_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_afrom_id_validates_async():
    """Test afrom_id validates asynchronously."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock(return_value={"name": "test"})
        MockDataService.return_value = mock_service

        result = await manager.afrom_id("async_service")

        mock_service.aget_metadata.assert_called_once()
        assert result == mock_service


@pytest.mark.asyncio
async def test_afrom_id_skips_validation():
    """Test afrom_id skips validation when validate=False."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock()
        MockDataService.return_value = mock_service

        await manager.afrom_id("async_service", validate=False)

        mock_service.aget_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_afrom_id_raises_resource_not_found():
    """Test afrom_id raises ResourceNotFoundError on validation failure."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock(
            side_effect=APIError("Not found"))
        MockDataService.return_value = mock_service

        with pytest.raises(ResourceNotFoundError):
            await manager.afrom_id("missing_async_service")


@pytest.mark.asyncio
async def test_afrom_key_is_alias():
    """Test afrom_key is alias for afrom_id."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock(return_value={})
        MockDataService.return_value = mock_service

        result = await manager.afrom_key("async_key")

        assert result == mock_service


class TestDataServiceManagerInvoke(unittest.TestCase):
    """Test cases for invoke method."""

    def test_invoke_calls_service_invoke(self):
        """Test invoke calls service.invoke."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.invoke.return_value = {"result": "data"}
            MockDataService.return_value = mock_service

            result = manager.invoke("test_service", {"input": "value"})

            mock_service.invoke.assert_called_once_with({"input": "value"})
            self.assertEqual(result, {"result": "data"})

    def test_invoke_combines_inputs_and_kwargs(self):
        """Test invoke combines inputs dict and kwargs."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.invoke.return_value = {}
            MockDataService.return_value = mock_service

            manager.invoke("test_service", {"a": 1}, b=2, c=3)

            call_args = mock_service.invoke.call_args[0][0]
            self.assertEqual(call_args, {"a": 1, "b": 2, "c": 3})

    def test_invoke_with_only_kwargs(self):
        """Test invoke with only kwargs."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.invoke.return_value = {}
            MockDataService.return_value = mock_service

            manager.invoke("test_service", param1="value1")

            call_args = mock_service.invoke.call_args[0][0]
            self.assertEqual(call_args, {"param1": "value1"})


@pytest.mark.asyncio
async def test_ainvoke_calls_service_ainvoke():
    """Test ainvoke calls service.ainvoke asynchronously."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock(return_value={})
        mock_service.ainvoke = AsyncMock(return_value={"async_result": "data"})
        MockDataService.return_value = mock_service

        result = await manager.ainvoke("test_service", {"input": "value"})

        mock_service.ainvoke.assert_called_once_with({"input": "value"})
        assert result == {"async_result": "data"}


class TestDataServiceManagerBatchInvoke(unittest.TestCase):
    """Test cases for batch_invoke_parallel method."""

    def test_batch_invoke_parallel_processes_all_inputs(self):
        """Test batch_invoke_parallel processes all inputs."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.invoke.side_effect = lambda x: {
                "result": x["value"] * 2
            }
            MockDataService.return_value = mock_service

            batch_inputs = [
                {
                    "value": 1
                },
                {
                    "value": 2
                },
                {
                    "value": 3
                },
            ]

            results = manager.batch_invoke_parallel("test_service",
                                                    batch_inputs)

            self.assertEqual(len(results), 3)
            self.assertEqual(mock_service.invoke.call_count, 3)

    def test_batch_invoke_parallel_maintains_order(self):
        """Test batch_invoke_parallel maintains input order."""
        mock_client = MagicMock()
        manager = DataServiceManager(mock_client)

        with patch('fusionbase.managers.dataservice_manager.DataService'
                  ) as MockDataService:
            mock_service = MagicMock()
            mock_service.invoke.side_effect = lambda x: {"index": x["idx"]}
            MockDataService.return_value = mock_service

            batch_inputs = [
                {
                    "idx": 0
                },
                {
                    "idx": 1
                },
                {
                    "idx": 2
                },
            ]

            results = manager.batch_invoke_parallel("test_service",
                                                    batch_inputs)

            for i, result in enumerate(results):
                self.assertEqual(result["index"], i)


@pytest.mark.asyncio
async def test_abatch_invoke_parallel_processes_all():
    """Test abatch_invoke_parallel processes all inputs asynchronously."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock(return_value={})
        mock_service.ainvoke = AsyncMock(
            side_effect=lambda x: {"result": x["value"]})
        MockDataService.return_value = mock_service

        batch_inputs = [{"value": 1}, {"value": 2}]

        results = await manager.abatch_invoke_parallel("test_service",
                                                       batch_inputs)

        assert len(results) == 2


@pytest.mark.asyncio
async def test_abatch_invoke_parallel_with_concurrency_limit():
    """Test abatch_invoke_parallel respects max_concurrency."""
    mock_client = MagicMock()
    manager = DataServiceManager(mock_client)

    with patch('fusionbase.managers.dataservice_manager.DataService'
              ) as MockDataService:
        mock_service = MagicMock()
        mock_service.aget_metadata = AsyncMock(return_value={})
        mock_service.ainvoke = AsyncMock(
            side_effect=lambda x: {"result": x["value"]})
        MockDataService.return_value = mock_service

        batch_inputs = [{"value": i} for i in range(5)]

        results = await manager.abatch_invoke_parallel("test_service",
                                                       batch_inputs,
                                                       max_concurrency=2)

        assert len(results) == 5
