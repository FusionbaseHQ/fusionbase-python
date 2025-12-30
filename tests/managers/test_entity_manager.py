"""Tests for EntityManager class."""

import unittest
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from fusionbase.entities.event import Event
from fusionbase.entities.location import Location
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person
from fusionbase.entities.relation import Relation
from fusionbase.managers.entity_manager import EntityManager
from fusionbase.managers.entity_managers import (
    EventManager,
    LocationManager,
    OrganizationManager,
    PersonManager,
    RelationManager,
)


class TestEntityManagerInitialization(unittest.TestCase):
    """Test cases for EntityManager initialization."""

    def test_entity_manager_creation(self):
        """Test creating EntityManager instance."""
        mock_client = MagicMock()

        manager = EntityManager(mock_client)

        self.assertEqual(manager.client, mock_client)

    def test_entity_manager_has_empty_entity_classes(self):
        """Test EntityManager starts with empty entity_classes dict."""
        mock_client = MagicMock()

        manager = EntityManager(mock_client)

        self.assertEqual(manager.entity_classes, {})

    def test_entity_manager_lazy_managers_are_none(self):
        """Test that type-specific managers start as None."""
        mock_client = MagicMock()

        manager = EntityManager(mock_client)

        self.assertIsNone(manager._locations)
        self.assertIsNone(manager._organizations)
        self.assertIsNone(manager._persons)
        self.assertIsNone(manager._events)
        self.assertIsNone(manager._relations)


class TestEntityManagerRegisterEntityClass(unittest.TestCase):
    """Test cases for register_entity_class method."""

    def test_register_entity_class(self):
        """Test registering an entity class."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        manager.register_entity_class("person", Person)

        self.assertIn("person", manager.entity_classes)
        self.assertEqual(manager.entity_classes["person"], Person)

    def test_register_multiple_entity_classes(self):
        """Test registering multiple entity classes."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        manager.register_entity_class("person", Person)
        manager.register_entity_class("organization", Organization)
        manager.register_entity_class("location", Location)

        self.assertEqual(len(manager.entity_classes), 3)
        self.assertEqual(manager.entity_classes["person"], Person)
        self.assertEqual(manager.entity_classes["organization"], Organization)
        self.assertEqual(manager.entity_classes["location"], Location)

    def test_register_overwrites_existing(self):
        """Test that registering overwrites existing class."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        manager.register_entity_class("test", Person)
        manager.register_entity_class("test", Organization)

        self.assertEqual(manager.entity_classes["test"], Organization)


class TestEntityManagerLocationProperty(unittest.TestCase):
    """Test cases for locations property."""

    def test_locations_property_creates_manager(self):
        """Test that locations property creates LocationManager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        locations = manager.locations

        self.assertIsInstance(locations, LocationManager)

    def test_locations_property_caches_manager(self):
        """Test that locations property returns cached manager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        locations1 = manager.locations
        locations2 = manager.locations

        self.assertIs(locations1, locations2)

    def test_locations_manager_has_correct_client(self):
        """Test that LocationManager has correct client."""
        mock_client = MagicMock()
        mock_client.name = "test_client"
        manager = EntityManager(mock_client)

        locations = manager.locations

        self.assertEqual(locations.client.name, "test_client")


class TestEntityManagerOrganizationsProperty(unittest.TestCase):
    """Test cases for organizations property."""

    def test_organizations_property_creates_manager(self):
        """Test that organizations property creates OrganizationManager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        organizations = manager.organizations

        self.assertIsInstance(organizations, OrganizationManager)

    def test_organizations_property_caches_manager(self):
        """Test that organizations property returns cached manager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        orgs1 = manager.organizations
        orgs2 = manager.organizations

        self.assertIs(orgs1, orgs2)


class TestEntityManagerPersonsProperty(unittest.TestCase):
    """Test cases for persons property."""

    def test_persons_property_creates_manager(self):
        """Test that persons property creates PersonManager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        persons = manager.persons

        self.assertIsInstance(persons, PersonManager)

    def test_persons_property_caches_manager(self):
        """Test that persons property returns cached manager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        persons1 = manager.persons
        persons2 = manager.persons

        self.assertIs(persons1, persons2)


class TestEntityManagerEventsProperty(unittest.TestCase):
    """Test cases for events property."""

    def test_events_property_creates_manager(self):
        """Test that events property creates EventManager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        events = manager.events

        self.assertIsInstance(events, EventManager)

    def test_events_property_caches_manager(self):
        """Test that events property returns cached manager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        events1 = manager.events
        events2 = manager.events

        self.assertIs(events1, events2)


class TestEntityManagerRelationsProperty(unittest.TestCase):
    """Test cases for relations property."""

    def test_relations_property_creates_manager(self):
        """Test that relations property creates RelationManager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        relations = manager.relations

        self.assertIsInstance(relations, RelationManager)

    def test_relations_property_caches_manager(self):
        """Test that relations property returns cached manager."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        relations1 = manager.relations
        relations2 = manager.relations

        self.assertIs(relations1, relations2)


class TestEntityManagerAfromId(unittest.TestCase):
    """Test cases for afrom_id method."""

    def test_afrom_id_raises_for_unknown_type(self):
        """Test afrom_id raises ValueError for unknown entity type."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        with self.assertRaises(ValueError) as context:
            import asyncio
            asyncio.run(manager.afrom_id("unknown_type", "123"))

        self.assertIn("Unknown entity type", str(context.exception))


@pytest.mark.asyncio
async def test_afrom_id_uses_entity_afrom_id():
    """Test afrom_id calls entity class _afrom_id."""
    mock_client = MagicMock()
    manager = EntityManager(mock_client)

    # Create a mock entity class
    mock_entity = MagicMock()
    mock_entity.fb_entity_id = "test_123"

    mock_entity_class = MagicMock()
    mock_entity_class._afrom_id = AsyncMock(return_value=mock_entity)

    manager.register_entity_class("test", mock_entity_class)

    result = await manager.afrom_id("test", "test_123")

    mock_entity_class._afrom_id.assert_called_once_with(mock_client, "test_123")
    assert result.fb_entity_id == "test_123"


@pytest.mark.asyncio
async def test_aget_is_alias_for_afrom_id():
    """Test aget method is alias for afrom_id."""
    mock_client = MagicMock()
    manager = EntityManager(mock_client)

    mock_entity = MagicMock()
    mock_entity_class = MagicMock()
    mock_entity_class._afrom_id = AsyncMock(return_value=mock_entity)

    manager.register_entity_class("test", mock_entity_class)

    result = await manager.aget("test", "123")

    mock_entity_class._afrom_id.assert_called_once()
    assert result == mock_entity


class TestEntityManagerAllManagers(unittest.TestCase):
    """Test that all manager properties work together."""

    def test_all_managers_independent(self):
        """Test that all managers are independent instances."""
        mock_client = MagicMock()
        manager = EntityManager(mock_client)

        locations = manager.locations
        organizations = manager.organizations
        persons = manager.persons
        events = manager.events
        relations = manager.relations

        # All should be different instances
        managers = [locations, organizations, persons, events, relations]
        for i, m1 in enumerate(managers):
            for j, m2 in enumerate(managers):
                if i != j:
                    self.assertIsNot(m1, m2)

    def test_all_managers_share_client(self):
        """Test that all managers share the same client."""
        mock_client = MagicMock()
        mock_client.id = "shared_client"
        manager = EntityManager(mock_client)

        self.assertEqual(manager.locations.client.id, "shared_client")
        self.assertEqual(manager.organizations.client.id, "shared_client")
        self.assertEqual(manager.persons.client.id, "shared_client")
        self.assertEqual(manager.events.client.id, "shared_client")
        self.assertEqual(manager.relations.client.id, "shared_client")


class TestSpecializedEntityManagers(unittest.TestCase):
    """Test specialized entity manager classes."""

    def test_location_manager_entity_class(self):
        """Test LocationManager has Location entity class."""
        mock_client = MagicMock()
        manager = LocationManager(mock_client, Location)

        self.assertEqual(manager.entity_class, Location)

    def test_organization_manager_entity_class(self):
        """Test OrganizationManager has Organization entity class."""
        mock_client = MagicMock()
        manager = OrganizationManager(mock_client, Organization)

        self.assertEqual(manager.entity_class, Organization)

    def test_person_manager_entity_class(self):
        """Test PersonManager has Person entity class."""
        mock_client = MagicMock()
        manager = PersonManager(mock_client, Person)

        self.assertEqual(manager.entity_class, Person)

    def test_event_manager_entity_class(self):
        """Test EventManager has Event entity class."""
        mock_client = MagicMock()
        manager = EventManager(mock_client, Event)

        self.assertEqual(manager.entity_class, Event)

    def test_relation_manager_entity_class(self):
        """Test RelationManager has Relation entity class."""
        mock_client = MagicMock()
        manager = RelationManager(mock_client, Relation)

        self.assertEqual(manager.entity_class, Relation)
