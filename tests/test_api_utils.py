"""Tests for API utility functions."""

import unittest
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from fusionbase.entities.base import Entity
from fusionbase.entities.person import Person
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.utils.api_utils import fetch_entity_async
from fusionbase.utils.api_utils import fetch_entity_sync
from fusionbase.utils.api_utils import make_entity_request
from fusionbase.utils.api_utils import make_entity_request_async


class TestMakeEntityRequest(unittest.TestCase):
    """Test cases for make_entity_request function."""

    def test_make_request_with_request_method(self):
        """Test make_entity_request using client.request method."""
        mock_client = MagicMock()
        mock_client.request.return_value = {"fb_entity_id": "123", "name": "Test"}

        result = make_entity_request(
            client=mock_client,
            entity_type="organization",
            entity_id="123"
        )

        mock_client.request.assert_called_once_with(
            "GET", "entities/organization/get/123"
        )
        self.assertEqual(result["fb_entity_id"], "123")

    def test_make_request_with_make_request_method(self):
        """Test make_entity_request using client.make_request method."""
        mock_client = MagicMock(spec=[])
        mock_client.make_request = MagicMock(return_value={"fb_entity_id": "456"})

        result = make_entity_request(
            client=mock_client,
            entity_type="person",
            entity_id="456"
        )

        mock_client.make_request.assert_called_once()
        self.assertEqual(result["fb_entity_id"], "456")

    def test_make_request_with_http_client_fallback(self):
        """Test make_entity_request fallback to http_client."""
        mock_client = MagicMock(spec=[])
        mock_response = MagicMock()
        mock_response.json.return_value = {"fb_entity_id": "789"}
        mock_client.http_client = MagicMock()
        mock_client.http_client.get.return_value = mock_response

        result = make_entity_request(
            client=mock_client,
            entity_type="location",
            entity_id="789"
        )

        mock_client.http_client.get.assert_called_once()
        self.assertEqual(result["fb_entity_id"], "789")

    def test_make_request_custom_endpoint_format(self):
        """Test make_entity_request with custom endpoint format."""
        mock_client = MagicMock()
        mock_client.request.return_value = {"fb_entity_id": "rel_1"}

        result = make_entity_request(
            client=mock_client,
            entity_type="relation",
            entity_id="rel_1",
            endpoint_format="relation/get/{}"
        )

        mock_client.request.assert_called_once_with(
            "GET", "relation/get/relation"  # First {} gets entity_type
        )

    def test_make_request_raises_resource_not_found(self):
        """Test make_entity_request raises ResourceNotFoundError on 404."""
        mock_client = MagicMock()
        mock_client.request.side_effect = ResourceNotFoundError()

        with self.assertRaises(ResourceNotFoundError) as context:
            make_entity_request(
                client=mock_client,
                entity_type="organization",
                entity_id="nonexistent"
            )

        self.assertEqual(context.exception.resource_type, "organization")
        self.assertEqual(context.exception.resource_id, "nonexistent")

    def test_make_request_raises_on_http_404(self):
        """Test make_entity_request handles HTTP 404 status."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client.request.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=mock_response
        )

        with self.assertRaises(ResourceNotFoundError):
            make_entity_request(
                client=mock_client,
                entity_type="person",
                entity_id="missing"
            )


@pytest.mark.asyncio
async def test_make_entity_request_async_with_aget():
    """Test async request with aget method."""
    mock_client = MagicMock()
    mock_client.aget = AsyncMock(return_value={"fb_entity_id": "async_1"})

    result = await make_entity_request_async(
        client=mock_client,
        entity_type="organization",
        entity_id="async_1"
    )

    mock_client.aget.assert_called_once()
    assert result["fb_entity_id"] == "async_1"


@pytest.mark.asyncio
async def test_make_entity_request_async_with_arequest():
    """Test async request with arequest method."""
    mock_client = MagicMock(spec=[])
    mock_client.arequest = AsyncMock(return_value={"fb_entity_id": "async_2"})

    result = await make_entity_request_async(
        client=mock_client,
        entity_type="person",
        entity_id="async_2"
    )

    mock_client.arequest.assert_called_once()
    assert result["fb_entity_id"] == "async_2"


@pytest.mark.asyncio
async def test_make_entity_request_async_with_async_http_client():
    """Test async request with _async_http_client."""
    mock_client = MagicMock(spec=[])
    mock_response = MagicMock()
    mock_response.json.return_value = {"fb_entity_id": "async_3"}
    mock_client._async_http_client = MagicMock()
    mock_client._async_http_client.get = AsyncMock(return_value=mock_response)

    result = await make_entity_request_async(
        client=mock_client,
        entity_type="location",
        entity_id="async_3"
    )

    mock_client._async_http_client.get.assert_called_once()
    assert result["fb_entity_id"] == "async_3"


@pytest.mark.asyncio
async def test_make_entity_request_async_raises_when_no_method():
    """Test async request raises APIError when no method available."""
    mock_client = MagicMock(spec=[])

    with pytest.raises(APIError) as exc_info:
        await make_entity_request_async(
            client=mock_client,
            entity_type="organization",
            entity_id="test"
        )

    assert "No suitable async method found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_make_entity_request_async_raises_resource_not_found():
    """Test async request raises ResourceNotFoundError."""
    mock_client = MagicMock()
    mock_client.aget = AsyncMock(side_effect=ResourceNotFoundError())

    with pytest.raises(ResourceNotFoundError) as exc_info:
        await make_entity_request_async(
            client=mock_client,
            entity_type="person",
            entity_id="missing"
        )

    assert exc_info.value.resource_type == "person"
    assert exc_info.value.resource_id == "missing"


class TestFetchEntitySync(unittest.TestCase):
    """Test cases for fetch_entity_sync function."""

    def test_fetch_entity_sync_creates_entity(self):
        """Test fetch_entity_sync creates entity instance."""
        mock_client = MagicMock()
        mock_client.request.return_value = {
            "fb_entity_id": "person_123",
            "fb_entity_version": "v1",
            "name": {
                "given": "John",
                "family": "Doe"
            }
        }

        result = fetch_entity_sync(Person, mock_client, "person_123")

        self.assertIsInstance(result, Person)
        self.assertEqual(result.fb_entity_id, "person_123")
        self.assertEqual(result.given_name, "John")

    def test_fetch_entity_sync_uses_correct_endpoint(self):
        """Test fetch_entity_sync uses correct endpoint for entity type."""
        mock_client = MagicMock()
        mock_client.request.return_value = {
            "fb_entity_id": "person_123",
            "fb_entity_version": "v1"
        }

        fetch_entity_sync(Person, mock_client, "person_123")

        mock_client.request.assert_called_once_with(
            "GET", "entities/person/get/person_123"
        )

    def test_fetch_entity_sync_stores_client_reference(self):
        """Test fetch_entity_sync stores client reference if available."""
        mock_client = MagicMock()
        mock_client.request.return_value = {
            "fb_entity_id": "test_123",
            "fb_entity_version": "v1"
        }

        # Create a test entity class with _client attribute
        class TestEntity(Entity):
            _client: MagicMock = None

        with patch.object(TestEntity, 'entity_type') as mock_type:
            mock_type.value = "test"
            result = fetch_entity_sync(TestEntity, mock_client, "test_123")

            if hasattr(result, "_client"):
                self.assertEqual(result._client, mock_client)


@pytest.mark.asyncio
async def test_fetch_entity_async_creates_entity():
    """Test fetch_entity_async creates entity instance."""
    mock_client = MagicMock()
    mock_client.aget = AsyncMock(return_value={
        "fb_entity_id": "async_person",
        "fb_entity_version": "v1",
        "name": {
            "given": "Jane",
            "family": "Smith"
        }
    })

    result = await fetch_entity_async(Person, mock_client, "async_person")

    assert isinstance(result, Person)
    assert result.fb_entity_id == "async_person"
    assert result.given_name == "Jane"


@pytest.mark.asyncio
async def test_fetch_entity_async_uses_correct_endpoint():
    """Test fetch_entity_async uses correct endpoint."""
    mock_client = MagicMock()
    mock_client.aget = AsyncMock(return_value={
        "fb_entity_id": "async_123",
        "fb_entity_version": "v1"
    })

    await fetch_entity_async(Person, mock_client, "async_123")

    mock_client.aget.assert_called_once_with("entities/person/get/async_123")


class TestEndpointFormatting(unittest.TestCase):
    """Test endpoint formatting for different entity types."""

    def test_organization_endpoint(self):
        """Test organization uses standard entity endpoint."""
        mock_client = MagicMock()
        mock_client.request.return_value = {
            "fb_entity_id": "org_1",
            "fb_entity_version": "v1"
        }

        from fusionbase.entities.organization import Organization
        fetch_entity_sync(Organization, mock_client, "org_1")

        call_args = mock_client.request.call_args
        self.assertIn("entities/organization/get/org_1", call_args[0])

    def test_location_endpoint(self):
        """Test location uses standard entity endpoint."""
        mock_client = MagicMock()
        mock_client.request.return_value = {
            "fb_entity_id": "loc_1",
            "fb_entity_version": "v1"
        }

        from fusionbase.entities.location import Location
        fetch_entity_sync(Location, mock_client, "loc_1")

        call_args = mock_client.request.call_args
        self.assertIn("entities/location/get/loc_1", call_args[0])


class TestErrorHandling(unittest.TestCase):
    """Test error handling in API utilities."""

    def test_make_request_wraps_generic_exception(self):
        """Test that generic exceptions are wrapped in APIError."""
        mock_client = MagicMock(spec=[])
        mock_client.http_client = MagicMock()
        mock_client.http_client.get.side_effect = Exception("Network error")

        with self.assertRaises(APIError) as context:
            make_entity_request(
                client=mock_client,
                entity_type="organization",
                entity_id="test"
            )

        self.assertIn("Failed to retrieve", str(context.exception))

    def test_resource_not_found_includes_details(self):
        """Test ResourceNotFoundError includes entity details."""
        mock_client = MagicMock()
        mock_client.request.side_effect = ResourceNotFoundError()

        with self.assertRaises(ResourceNotFoundError) as context:
            make_entity_request(
                client=mock_client,
                entity_type="person",
                entity_id="missing_id"
            )

        error = context.exception
        self.assertEqual(error.resource_type, "person")
        self.assertEqual(error.resource_id, "missing_id")
        self.assertEqual(error.status_code, 404)
