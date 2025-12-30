"""Tests for SearchManager class."""

import unittest
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase.managers.search_manager import SearchManager
from fusionbase.managers.search_wrappers import DataSearch
from fusionbase.managers.search_wrappers import FusionSearch
from fusionbase.managers.search_wrappers import LocationSearch
from fusionbase.managers.search_wrappers import OrganizationSearch
from fusionbase.managers.search_wrappers import PersonSearch
from fusionbase.managers.search_wrappers import RelationSearch


class TestSearchManagerInitialization(unittest.TestCase):
    """Test cases for SearchManager initialization."""

    def test_manager_creation(self):
        """Test creating SearchManager instance."""
        mock_client = MagicMock()

        manager = SearchManager(mock_client)

        self.assertEqual(manager._client, mock_client)
        self.assertEqual(manager._search_classes, {})

    def test_manager_lazy_properties_are_none(self):
        """Test that all lazy properties start as None."""
        mock_client = MagicMock()

        manager = SearchManager(mock_client)

        self.assertIsNone(manager._locations)
        self.assertIsNone(manager._persons)
        self.assertIsNone(manager._organizations)
        self.assertIsNone(manager._relations)
        self.assertIsNone(manager._data)
        self.assertIsNone(manager._fusion)

    def test_manager_async_properties_are_none(self):
        """Test that async search instances start as None."""
        mock_client = MagicMock()

        manager = SearchManager(mock_client)

        self.assertIsNone(manager._async_loc_search)
        self.assertIsNone(manager._async_person_search)
        self.assertIsNone(manager._async_org_search)
        self.assertIsNone(manager._async_rel_search)
        self.assertIsNone(manager._async_data_search)
        self.assertIsNone(manager._async_fusion_search)


class TestSearchManagerRegisterSearchClass(unittest.TestCase):
    """Test cases for register_search_class method."""

    def test_register_search_class(self):
        """Test registering a search class."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)
        mock_search_class = MagicMock()

        manager.register_search_class("custom", mock_search_class)

        self.assertIn("custom", manager._search_classes)
        self.assertEqual(manager._search_classes["custom"], mock_search_class)

    def test_register_multiple_search_classes(self):
        """Test registering multiple search classes."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        manager.register_search_class("type1", MagicMock())
        manager.register_search_class("type2", MagicMock())

        self.assertEqual(len(manager._search_classes), 2)


class TestSearchManagerLocationsProperty(unittest.TestCase):
    """Test cases for locations property."""

    def test_locations_property_creates_search(self):
        """Test that locations property creates LocationSearch."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        locations = manager.locations

        self.assertIsInstance(locations, LocationSearch)

    def test_locations_property_caches_search(self):
        """Test that locations property returns cached instance."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        locations1 = manager.locations
        locations2 = manager.locations

        self.assertIs(locations1, locations2)


class TestSearchManagerPersonsProperty(unittest.TestCase):
    """Test cases for persons property."""

    def test_persons_property_creates_search(self):
        """Test that persons property creates PersonSearch."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        persons = manager.persons

        self.assertIsInstance(persons, PersonSearch)

    def test_persons_property_caches_search(self):
        """Test that persons property returns cached instance."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        persons1 = manager.persons
        persons2 = manager.persons

        self.assertIs(persons1, persons2)


class TestSearchManagerOrganizationsProperty(unittest.TestCase):
    """Test cases for organizations property."""

    def test_organizations_property_creates_search(self):
        """Test that organizations property creates OrganizationSearch."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        organizations = manager.organizations

        self.assertIsInstance(organizations, OrganizationSearch)

    def test_organizations_property_caches_search(self):
        """Test that organizations property returns cached instance."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        orgs1 = manager.organizations
        orgs2 = manager.organizations

        self.assertIs(orgs1, orgs2)


class TestSearchManagerRelationsProperty(unittest.TestCase):
    """Test cases for relations property."""

    def test_relations_property_creates_search(self):
        """Test that relations property creates RelationSearch."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        relations = manager.relations

        self.assertIsInstance(relations, RelationSearch)

    def test_relations_property_caches_search(self):
        """Test that relations property returns cached instance."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        relations1 = manager.relations
        relations2 = manager.relations

        self.assertIs(relations1, relations2)


class TestSearchManagerDataProperty(unittest.TestCase):
    """Test cases for data property."""

    def test_data_property_creates_search(self):
        """Test that data property creates DataSearch."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        data = manager.data

        self.assertIsInstance(data, DataSearch)

    def test_data_property_caches_search(self):
        """Test that data property returns cached instance."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        data1 = manager.data
        data2 = manager.data

        self.assertIs(data1, data2)


class TestSearchManagerFusionProperty(unittest.TestCase):
    """Test cases for fusion property."""

    def test_fusion_property_creates_search(self):
        """Test that fusion property creates FusionSearch."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        fusion = manager.fusion

        self.assertIsInstance(fusion, FusionSearch)

    def test_fusion_property_caches_search(self):
        """Test that fusion property returns cached instance."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        fusion1 = manager.fusion
        fusion2 = manager.fusion

        self.assertIs(fusion1, fusion2)


class TestSearchManagerAllProperties(unittest.TestCase):
    """Test that all search properties work together."""

    def test_all_properties_independent(self):
        """Test that all search properties are independent instances."""
        mock_client = MagicMock()
        manager = SearchManager(mock_client)

        locations = manager.locations
        persons = manager.persons
        organizations = manager.organizations
        relations = manager.relations
        data = manager.data
        fusion = manager.fusion

        # All should be different instances
        searches = [locations, persons, organizations, relations, data, fusion]
        for i, s1 in enumerate(searches):
            for j, s2 in enumerate(searches):
                if i != j:
                    self.assertIsNot(s1, s2)

    def test_all_properties_share_client(self):
        """Test that all search instances share the same client."""
        mock_client = MagicMock()
        mock_client.name = "shared_client"
        manager = SearchManager(mock_client)

        # Access all properties
        _ = manager.locations
        _ = manager.persons
        _ = manager.organizations
        _ = manager.relations
        _ = manager.data
        _ = manager.fusion

        # Verify client is stored in manager
        self.assertEqual(manager._client.name, "shared_client")


@pytest.mark.asyncio
async def test_asearch_persons():
    """Test asearch_persons async method."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    mock_result = MagicMock()
    with patch.object(PersonSearch, 'asearch', new_callable=AsyncMock, return_value=mock_result):
        result = await manager.asearch_persons(query="John")

        assert result == mock_result


@pytest.mark.asyncio
async def test_asearch_persons_caches_search_instance():
    """Test asearch_persons caches the search instance."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    assert manager._async_person_search is None

    with patch.object(PersonSearch, 'asearch', new_callable=AsyncMock, return_value=MagicMock()):
        await manager.asearch_persons(query="Test")

        assert manager._async_person_search is not None


@pytest.mark.asyncio
async def test_asearch_organizations():
    """Test asearch_organizations async method."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    mock_result = MagicMock()
    with patch.object(OrganizationSearch, 'asearch', new_callable=AsyncMock, return_value=mock_result):
        result = await manager.asearch_organizations(query="Acme")

        assert result == mock_result


@pytest.mark.asyncio
async def test_asearch_locations():
    """Test asearch_locations async method."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    mock_result = MagicMock()
    with patch.object(LocationSearch, 'asearch', new_callable=AsyncMock, return_value=mock_result):
        result = await manager.asearch_locations(query="New York")

        assert result == mock_result


@pytest.mark.asyncio
async def test_asearch_relations():
    """Test asearch_relations async method."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    mock_result = MagicMock()
    with patch.object(RelationSearch, 'asearch', new_callable=AsyncMock, return_value=mock_result):
        result = await manager.asearch_relations(query="director")

        assert result == mock_result


@pytest.mark.asyncio
async def test_asearch_data():
    """Test asearch_data async method."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    mock_result = MagicMock()
    with patch.object(DataSearch, 'asearch', new_callable=AsyncMock, return_value=mock_result):
        result = await manager.asearch_data(query="dataset")

        assert result == mock_result


@pytest.mark.asyncio
async def test_asearch_fusion():
    """Test asearch_fusion async method."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    mock_result = MagicMock()
    with patch.object(FusionSearch, 'asearch', new_callable=AsyncMock, return_value=mock_result):
        result = await manager.asearch_fusion(query="everything")

        assert result == mock_result


@pytest.mark.asyncio
async def test_async_methods_cache_independently():
    """Test that async methods cache their search instances independently."""
    mock_client = MagicMock()
    manager = SearchManager(mock_client)

    with patch.object(PersonSearch, 'asearch', new_callable=AsyncMock, return_value=MagicMock()):
        await manager.asearch_persons(query="Test")

    with patch.object(OrganizationSearch, 'asearch', new_callable=AsyncMock, return_value=MagicMock()):
        await manager.asearch_organizations(query="Test")

    # Both should be cached independently
    assert manager._async_person_search is not None
    assert manager._async_org_search is not None
    assert manager._async_person_search is not manager._async_org_search
