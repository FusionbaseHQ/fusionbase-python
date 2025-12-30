"""Tests for the base Entity class."""

import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase.core.context import set_current_entity_manager
from fusionbase.entities.base import Entity
from fusionbase.types.entities import EntityType


class TestEntityBasics(unittest.TestCase):
    """Test cases for basic Entity functionality."""

    def test_entity_required_fields(self):
        """Test that Entity requires fb_entity_id and fb_entity_version."""
        entity = Entity(
            fb_entity_id="test_123",
            fb_entity_version="v1"
        )

        self.assertEqual(entity.fb_entity_id, "test_123")
        self.assertEqual(entity.fb_entity_version, "v1")

    def test_entity_optional_fields_defaults(self):
        """Test that optional fields have correct defaults."""
        entity = Entity(
            fb_entity_id="test_123",
            fb_entity_version="v1"
        )

        self.assertIsNone(entity.name)
        self.assertEqual(entity.metadata, {})
        self.assertEqual(entity.external_ids, {})

    def test_entity_with_name(self):
        """Test Entity with name field."""
        entity = Entity(
            fb_entity_id="test_123",
            fb_entity_version="v1",
            name="Test Entity"
        )

        self.assertEqual(entity.name, "Test Entity")

    def test_entity_with_metadata(self):
        """Test Entity with metadata."""
        metadata = {"source": "api", "version": 2}
        entity = Entity(
            fb_entity_id="test_123",
            fb_entity_version="v1",
            metadata=metadata
        )

        self.assertEqual(entity.metadata, {"source": "api", "version": 2})

    def test_entity_with_external_ids(self):
        """Test Entity with external_ids."""
        external_ids = {"crm": "CRM123", "erp": "ERP456"}
        entity = Entity(
            fb_entity_id="test_123",
            fb_entity_version="v1",
            external_ids=external_ids
        )

        self.assertEqual(entity.external_ids["crm"], "CRM123")
        self.assertEqual(entity.external_ids["erp"], "ERP456")

    def test_entity_default_entity_type(self):
        """Test that Entity has default entity_type."""
        # Default is ORGANIZATION as per the base class
        self.assertEqual(Entity.entity_type, EntityType.ORGANIZATION)

    def test_entity_model_validation(self):
        """Test Entity creation via model_validate."""
        data = {
            "fb_entity_id": "validated_123",
            "fb_entity_version": "v2",
            "name": "Validated Entity",
            "metadata": {"key": "value"}
        }

        entity = Entity.model_validate(data)

        self.assertEqual(entity.fb_entity_id, "validated_123")
        self.assertEqual(entity.fb_entity_version, "v2")
        self.assertEqual(entity.name, "Validated Entity")

    def test_entity_model_dump(self):
        """Test Entity serialization via model_dump."""
        entity = Entity(
            fb_entity_id="dump_123",
            fb_entity_version="v1",
            name="Dump Test"
        )

        dumped = entity.model_dump()

        self.assertIsInstance(dumped, dict)
        self.assertEqual(dumped["fb_entity_id"], "dump_123")
        self.assertEqual(dumped["name"], "Dump Test")


class TestEntityGetMethod(unittest.TestCase):
    """Test cases for Entity.get() class method."""

    def setUp(self):
        """Set up test fixtures."""
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up after tests."""
        set_current_entity_manager(None)

    def test_get_raises_without_context(self):
        """Test that get() raises RuntimeError without entity manager context."""
        with self.assertRaises(RuntimeError) as context:
            Entity.get("test_id")

        self.assertIn("No entity manager available", str(context.exception))
        self.assertIn("with statement", str(context.exception))

    def test_get_uses_entity_manager(self):
        """Test that get() uses the entity manager from context."""
        mock_manager = MagicMock()
        mock_entity = Entity(fb_entity_id="result_123", fb_entity_version="v1")
        mock_manager.get.return_value = mock_entity

        set_current_entity_manager(mock_manager)

        result = Entity.get("test_id")

        mock_manager.get.assert_called_once_with("organization", "test_id")
        self.assertEqual(result.fb_entity_id, "result_123")


class TestEntityAgetMethod(unittest.TestCase):
    """Test cases for Entity.aget() async class method."""

    def setUp(self):
        """Set up test fixtures."""
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up after tests."""
        set_current_entity_manager(None)


@pytest.mark.asyncio
async def test_aget_raises_without_context():
    """Test that aget() raises RuntimeError without entity manager context."""
    set_current_entity_manager(None)

    with pytest.raises(RuntimeError) as exc_info:
        await Entity.aget("test_id")

    assert "No entity manager available" in str(exc_info.value)


@pytest.mark.asyncio
async def test_aget_uses_entity_manager():
    """Test that aget() uses the entity manager from context."""
    mock_manager = MagicMock()
    mock_entity = Entity(fb_entity_id="async_result", fb_entity_version="v1")

    async def mock_afrom_id(entity_type, entity_id):
        return mock_entity

    mock_manager.afrom_id = mock_afrom_id

    set_current_entity_manager(mock_manager)

    try:
        result = await Entity.aget("test_id")
        assert result.fb_entity_id == "async_result"
    finally:
        set_current_entity_manager(None)


class TestEntityGetRelations(unittest.TestCase):
    """Test cases for Entity.get_relations() method."""

    def setUp(self):
        """Set up test fixtures."""
        set_current_entity_manager(None)

    def tearDown(self):
        """Clean up after tests."""
        set_current_entity_manager(None)

    def test_get_relations_raises_without_context(self):
        """Test get_relations() raises without context and no client."""
        entity = Entity(fb_entity_id="test_123", fb_entity_version="v1")

        with self.assertRaises(RuntimeError) as context:
            entity.get_relations()

        self.assertIn("No client provided", str(context.exception))

    def test_get_relations_with_explicit_client(self):
        """Test get_relations() with explicit client parameter."""
        mock_client = MagicMock()
        mock_client.request.return_value = []

        entity = Entity(fb_entity_id="test_123", fb_entity_version="v1")

        # This will call list_relations with the provided client
        with patch.object(Entity, 'list_relations', return_value=[]) as mock_list:
            result = entity.get_relations(client=mock_client)

            mock_list.assert_called_once_with(mock_client)
            self.assertEqual(result, [])

    def test_get_relations_uses_manager_client(self):
        """Test get_relations() uses client from entity manager."""
        mock_client = MagicMock()
        mock_manager = MagicMock()
        mock_manager.client = mock_client

        set_current_entity_manager(mock_manager)

        entity = Entity(fb_entity_id="test_123", fb_entity_version="v1")

        with patch.object(Entity, 'list_relations', return_value=[]) as mock_list:
            entity.get_relations()

            mock_list.assert_called_once_with(mock_client)


class TestEntityListRelations(unittest.TestCase):
    """Test cases for Entity.list_relations() class method."""

    def test_list_relations_calls_api(self):
        """Test list_relations() makes correct API call."""
        mock_client = MagicMock()
        mock_client.request.return_value = []

        Entity.list_relations(mock_client)

        mock_client.request.assert_called_once_with(
            "GET", "relation/list/organization"
        )

    def test_list_relations_parses_response(self):
        """Test list_relations() correctly parses API response."""
        mock_client = MagicMock()
        mock_client.request.return_value = [
            {
                "id": "rel_1",
                "key": "test_relation",
                "name": {"en": "Test Relation"},
                "description": {"en": "A test relation"},
                "label": "TEST_RELATION",
                "model_from": "organization",
                "model_to": "person",
                "meta": {},
                "resolve": {"parameter_definition": []},
                "created_at": "2024-01-01",
                "updated_at": "2024-01-02"
            }
        ]

        relations = Entity.list_relations(mock_client)

        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].fb_entity_id, "rel_1")
        self.assertEqual(relations[0].label, "TEST_RELATION")

    def test_list_relations_uses_http_client_fallback(self):
        """Test list_relations() falls back to http_client."""
        mock_client = MagicMock(spec=[])  # No 'request' attribute
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_client.http_client = MagicMock()
        mock_client.http_client.get.return_value = mock_response

        relations = Entity.list_relations(mock_client)

        mock_client.http_client.get.assert_called_once()
        self.assertEqual(relations, [])


class TestEntityFromId(unittest.TestCase):
    """Test cases for Entity._from_id() method."""

    def test_from_id_mock(self):
        """Test _from_id() with mocked API."""
        mock_client = MagicMock()

        with patch('fusionbase.utils.api_utils.fetch_entity_sync') as mock_fetch:
            mock_entity = Entity(fb_entity_id="fetched_123", fb_entity_version="v1")
            mock_fetch.return_value = mock_entity

            result = Entity._from_id(mock_client, "test_id")

            mock_fetch.assert_called_once_with(Entity, mock_client, "test_id")
            self.assertEqual(result.fb_entity_id, "fetched_123")


@pytest.mark.asyncio
async def test_afrom_id_mock():
    """Test _afrom_id() with mocked API."""
    mock_client = MagicMock()

    with patch('fusionbase.utils.api_utils.fetch_entity_async') as mock_fetch:
        mock_entity = Entity(fb_entity_id="async_fetched", fb_entity_version="v1")
        mock_fetch.return_value = mock_entity

        result = await Entity._afrom_id(mock_client, "async_id")

        mock_fetch.assert_called_once_with(Entity, mock_client, "async_id")
        assert result.fb_entity_id == "async_fetched"


class TestEntityInheritance(unittest.TestCase):
    """Test Entity class inheritance capabilities."""

    def test_subclass_can_override_entity_type(self):
        """Test that subclasses can override entity_type."""
        from fusionbase.entities.location import Location
        from fusionbase.entities.person import Person

        self.assertEqual(Person.entity_type, EntityType.PERSON)
        self.assertEqual(Location.entity_type, EntityType.LOCATION)

    def test_subclass_inherits_methods(self):
        """Test that subclasses inherit base methods."""
        from fusionbase.entities.person import Person

        # Person should have inherited methods
        self.assertTrue(hasattr(Person, 'get'))
        self.assertTrue(hasattr(Person, 'aget'))
        self.assertTrue(hasattr(Person, 'get_relations'))
        self.assertTrue(hasattr(Person, 'list_relations'))


class TestEntityModelConfig(unittest.TestCase):
    """Test Entity model configuration."""

    def test_entity_allows_arbitrary_types(self):
        """Test that Entity model allows arbitrary types."""
        self.assertTrue(Entity.model_config.get('arbitrary_types_allowed'))

    def test_entity_can_have_extra_fields(self):
        """Test Entity handles extra fields in data."""
        data = {
            "fb_entity_id": "extra_123",
            "fb_entity_version": "v1",
            "extra_field": "extra_value"
        }

        # Should not raise, extra fields are allowed by default in pydantic v2
        entity = Entity.model_validate(data)

        self.assertEqual(entity.fb_entity_id, "extra_123")
