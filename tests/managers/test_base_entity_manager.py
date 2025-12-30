"""Tests for BaseEntityManager class."""

import asyncio
import unittest
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase.entities.base import Entity
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.managers.base_entity_manager import BaseEntityManager


class TestBaseEntityManagerInitialization(unittest.TestCase):
    """Test cases for BaseEntityManager initialization."""

    def test_manager_creation(self):
        """Test creating BaseEntityManager instance."""
        mock_client = MagicMock()

        manager = BaseEntityManager(mock_client, Person)

        self.assertEqual(manager.client, mock_client)
        self.assertEqual(manager.entity_class, Person)

    def test_manager_with_different_entity_class(self):
        """Test creating manager with different entity classes."""
        mock_client = MagicMock()

        person_manager = BaseEntityManager(mock_client, Person)
        org_manager = BaseEntityManager(mock_client, Organization)

        self.assertEqual(person_manager.entity_class, Person)
        self.assertEqual(org_manager.entity_class, Organization)


class TestBaseEntityManagerGet(unittest.TestCase):
    """Test cases for get method."""

    def test_get_calls_entity_from_id(self):
        """Test get method calls entity class _from_id."""
        mock_client = MagicMock()
        manager = BaseEntityManager(mock_client, Person)

        mock_person = MagicMock(spec=Person)
        with patch.object(Person, '_from_id',
                          return_value=mock_person) as mock_from_id:
            result = manager.get("person_123")

            mock_from_id.assert_called_once_with(mock_client, "person_123")
            self.assertEqual(result, mock_person)

    def test_get_propagates_resource_not_found_error(self):
        """Test get propagates ResourceNotFoundError."""
        mock_client = MagicMock()
        manager = BaseEntityManager(mock_client, Person)

        with patch.object(Person,
                          '_from_id',
                          side_effect=ResourceNotFoundError(
                              "person", "missing")):
            with self.assertRaises(ResourceNotFoundError):
                manager.get("missing")

    def test_get_propagates_api_error(self):
        """Test get propagates APIError."""
        mock_client = MagicMock()
        manager = BaseEntityManager(mock_client, Person)

        with patch.object(Person,
                          '_from_id',
                          side_effect=APIError("API failed")):
            with self.assertRaises(APIError):
                manager.get("test_id")


class TestBaseEntityManagerFromId(unittest.TestCase):
    """Test cases for from_id method."""

    def test_from_id_is_alias_for_get(self):
        """Test from_id calls get method."""
        mock_client = MagicMock()
        manager = BaseEntityManager(mock_client, Person)

        mock_person = MagicMock(spec=Person)
        with patch.object(Person, '_from_id', return_value=mock_person):
            result = manager.from_id("person_456")

            self.assertEqual(result, mock_person)

    def test_from_id_returns_same_as_get(self):
        """Test from_id returns same result as get."""
        mock_client = MagicMock()
        manager = BaseEntityManager(mock_client, Person)

        mock_person = MagicMock(spec=Person)
        mock_person.fb_entity_id = "test_id"
        with patch.object(Person, '_from_id', return_value=mock_person):
            get_result = manager.get("test_id")
            from_id_result = manager.from_id("test_id")

            # Both should be the same mock object
            self.assertIs(get_result, from_id_result)


@pytest.mark.asyncio
async def test_aget_with_afrom_id_method():
    """Test aget uses _afrom_id when available."""
    mock_client = MagicMock()
    manager = BaseEntityManager(mock_client, Person)

    mock_person = MagicMock(spec=Person)

    with patch.object(Person,
                      '_afrom_id',
                      new_callable=AsyncMock,
                      return_value=mock_person):
        result = await manager.aget("async_person_123")

        Person._afrom_id.assert_called_once_with(mock_client,
                                                 "async_person_123")
        assert result == mock_person


@pytest.mark.asyncio
async def test_aget_falls_back_to_sync():
    """Test aget falls back to sync _from_id via to_thread."""
    mock_client = MagicMock()

    # Create a mock entity class without _afrom_id
    class MockEntity:

        @classmethod
        def _from_id(cls, client, entity_id):
            return MagicMock(fb_entity_id=entity_id)

    manager = BaseEntityManager(mock_client, MockEntity)

    result = await manager.aget("sync_entity_123")

    assert result.fb_entity_id == "sync_entity_123"


@pytest.mark.asyncio
async def test_afrom_id_is_alias_for_aget():
    """Test afrom_id calls aget method."""
    mock_client = MagicMock()
    manager = BaseEntityManager(mock_client, Person)

    mock_person = MagicMock(spec=Person)

    with patch.object(Person,
                      '_afrom_id',
                      new_callable=AsyncMock,
                      return_value=mock_person):
        result = await manager.afrom_id("async_456")

        assert result == mock_person


@pytest.mark.asyncio
async def test_aget_propagates_resource_not_found_error():
    """Test aget propagates ResourceNotFoundError."""
    mock_client = MagicMock()
    manager = BaseEntityManager(mock_client, Person)

    with patch.object(Person,
                      '_afrom_id',
                      new_callable=AsyncMock,
                      side_effect=ResourceNotFoundError("person", "missing")):
        with pytest.raises(ResourceNotFoundError):
            await manager.aget("missing")


class TestBaseEntityManagerMakeRequest(unittest.TestCase):
    """Test cases for _make_request method."""

    def test_make_request_get_method(self):
        """Test _make_request with GET method."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "value"}
        mock_client._http_client.get.return_value = mock_response

        manager = BaseEntityManager(mock_client, Person)

        result = manager._make_request("/test/path", GET=True)

        mock_client._http_client.get.assert_called_once()
        self.assertEqual(result, {"data": "value"})

    def test_make_request_other_method(self):
        """Test _make_request with non-GET method."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": "ok"}
        mock_client._http_client.request.return_value = mock_response

        manager = BaseEntityManager(mock_client, Person)

        result = manager._make_request("/test/path", method="POST")

        mock_client._http_client.request.assert_called_once()
        self.assertEqual(result, {"result": "ok"})

    def test_make_request_raises_for_status(self):
        """Test _make_request calls raise_for_status."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_client._http_client.request.return_value = mock_response

        manager = BaseEntityManager(mock_client, Person)

        manager._make_request("/test/path")

        mock_response.raise_for_status.assert_called_once()

    def test_make_request_re_raises_api_error(self):
        """Test _make_request re-raises APIError."""
        mock_client = MagicMock()
        mock_client._http_client.request.side_effect = APIError("API failed")

        manager = BaseEntityManager(mock_client, Person)

        with self.assertRaises(APIError):
            manager._make_request("/test/path")

    def test_make_request_re_raises_resource_not_found_error(self):
        """Test _make_request re-raises ResourceNotFoundError."""
        mock_client = MagicMock()
        mock_client._http_client.request.side_effect = ResourceNotFoundError(
            "entity", "id")

        manager = BaseEntityManager(mock_client, Person)

        with self.assertRaises(ResourceNotFoundError):
            manager._make_request("/test/path")

    def test_make_request_wraps_generic_exception(self):
        """Test _make_request wraps generic exceptions in APIError."""
        mock_client = MagicMock()
        mock_client._http_client.request.side_effect = Exception(
            "Network error")

        manager = BaseEntityManager(mock_client, Person)

        with self.assertRaises(APIError) as context:
            manager._make_request("/test/path")

        self.assertIn("Error making request", str(context.exception))

    def test_make_request_extracts_status_code_from_exception(self):
        """Test _make_request extracts status code from exception response."""
        mock_client = MagicMock()
        mock_exception = Exception("HTTP error")
        mock_exception.response = MagicMock()
        mock_exception.response.status_code = 500
        mock_client._http_client.request.side_effect = mock_exception

        manager = BaseEntityManager(mock_client, Person)

        with self.assertRaises(APIError) as context:
            manager._make_request("/test/path")

        self.assertEqual(context.exception.status_code, 500)


class TestBaseEntityManagerGenericType(unittest.TestCase):
    """Test that BaseEntityManager works as a generic type."""

    def test_manager_type_parameter(self):
        """Test manager works with type parameter."""
        mock_client = MagicMock()

        # Create managers with specific types
        person_manager: BaseEntityManager[Person] = BaseEntityManager(
            mock_client, Person)
        org_manager: BaseEntityManager[Organization] = BaseEntityManager(
            mock_client, Organization)

        # Type hints should work correctly
        self.assertEqual(person_manager.entity_class, Person)
        self.assertEqual(org_manager.entity_class, Organization)
