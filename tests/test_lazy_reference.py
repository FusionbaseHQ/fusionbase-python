"""Tests for LazyReference functionality."""

import unittest
from unittest.mock import MagicMock, AsyncMock

import pytest

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.person import Person


class MockEntity:
    """Mock entity for testing LazyReference."""

    def __init__(self, entity_id: str, name: str):
        self.fb_entity_id = entity_id
        self.name = name

    @classmethod
    def _from_id(cls, client, entity_id: str):
        """Mock sync loader."""
        return cls(entity_id, f"Entity_{entity_id}")

    @classmethod
    async def _afrom_id(cls, client, entity_id: str):
        """Mock async loader."""
        return cls(entity_id, f"AsyncEntity_{entity_id}")


class TestLazyReference(unittest.TestCase):
    """Test cases for LazyReference."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.entity_id = "test_entity_123"

    def test_lazy_reference_creation(self):
        """Test creating a LazyReference."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )
        self.assertEqual(ref.entity_id, self.entity_id)
        self.assertFalse(ref.is_loaded)

    def test_lazy_reference_entity_id_property(self):
        """Test entity_id property returns correct ID."""
        ref = LazyReference(
            entity_id="my_id_123",
            entity_cls=MockEntity,
            client=self.mock_client
        )
        self.assertEqual(ref.entity_id, "my_id_123")

    def test_lazy_reference_is_loaded_before_get(self):
        """Test is_loaded is False before calling get()."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )
        self.assertFalse(ref.is_loaded)

    def test_lazy_reference_get_loads_entity(self):
        """Test get() loads and returns the entity."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )

        entity = ref.get()

        self.assertTrue(ref.is_loaded)
        self.assertEqual(entity.fb_entity_id, self.entity_id)
        self.assertEqual(entity.name, f"Entity_{self.entity_id}")

    def test_lazy_reference_get_caches_entity(self):
        """Test get() caches the entity and doesn't reload."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )

        # First call loads the entity
        entity1 = ref.get()
        # Second call should return the cached entity
        entity2 = ref.get()

        self.assertIs(entity1, entity2)

    def test_lazy_reference_custom_loader(self):
        """Test LazyReference with custom loader function."""
        custom_entity = MockEntity("custom_id", "CustomName")

        def custom_loader(client, entity_id):
            return custom_entity

        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client,
            loader_func=custom_loader
        )

        entity = ref.get()

        self.assertEqual(entity.name, "CustomName")
        self.assertIs(entity, custom_entity)

    def test_lazy_reference_getattr_passes_through(self):
        """Test __getattr__ transparently passes to loaded entity."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )

        # Accessing attribute should trigger load and return entity's attribute
        name = ref.name

        self.assertTrue(ref.is_loaded)
        self.assertEqual(name, f"Entity_{self.entity_id}")

    def test_lazy_reference_str_unloaded(self):
        """Test string representation when unloaded."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )

        str_repr = str(ref)

        self.assertIn("unloaded", str_repr)
        self.assertIn("MockEntity", str_repr)
        self.assertIn(self.entity_id, str_repr)

    def test_lazy_reference_str_loaded(self):
        """Test string representation when loaded."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )
        ref.get()  # Load the entity

        str_repr = str(ref)

        self.assertIn("loaded", str_repr)

    def test_lazy_reference_repr_unloaded(self):
        """Test repr when unloaded."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )

        repr_str = repr(ref)

        self.assertIn("unloaded", repr_str)
        self.assertIn("MockEntity", repr_str)

    def test_lazy_reference_repr_loaded(self):
        """Test repr when loaded."""
        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client
        )
        ref.get()

        repr_str = repr(ref)

        self.assertIn("loaded", repr_str)

    def test_lazy_reference_load_error_cached(self):
        """Test that load errors are cached and re-raised."""
        def failing_loader(client, entity_id):
            raise ValueError("Load failed")

        ref = LazyReference(
            entity_id=self.entity_id,
            entity_cls=MockEntity,
            client=self.mock_client,
            loader_func=failing_loader
        )

        # First call raises the error
        with self.assertRaises(ValueError):
            ref.get()

        # Second call should re-raise the cached error
        with self.assertRaises(ValueError):
            ref.get()


@pytest.mark.asyncio
async def test_lazy_reference_aget():
    """Test async get() loads and returns the entity."""
    mock_client = MagicMock()
    entity_id = "async_test_123"

    ref = LazyReference(
        entity_id=entity_id,
        entity_cls=MockEntity,
        client=mock_client
    )

    entity = await ref.aget()

    assert ref.is_loaded
    assert entity.fb_entity_id == entity_id
    assert entity.name == f"AsyncEntity_{entity_id}"


@pytest.mark.asyncio
async def test_lazy_reference_aget_caches_entity():
    """Test async get() caches the entity."""
    mock_client = MagicMock()
    entity_id = "async_cache_test"

    ref = LazyReference(
        entity_id=entity_id,
        entity_cls=MockEntity,
        client=mock_client
    )

    entity1 = await ref.aget()
    entity2 = await ref.aget()

    assert entity1 is entity2


@pytest.mark.asyncio
async def test_lazy_reference_custom_async_loader():
    """Test LazyReference with custom async loader function."""
    mock_client = MagicMock()
    custom_entity = MockEntity("async_custom", "AsyncCustomName")

    async def custom_async_loader(client, entity_id):
        return custom_entity

    ref = LazyReference(
        entity_id="any_id",
        entity_cls=MockEntity,
        client=mock_client,
        async_loader_func=custom_async_loader
    )

    entity = await ref.aget()

    assert entity.name == "AsyncCustomName"
    assert entity is custom_entity


@pytest.mark.asyncio
async def test_lazy_reference_async_load_error_cached():
    """Test that async load errors are cached and re-raised."""
    mock_client = MagicMock()

    async def failing_async_loader(client, entity_id):
        raise RuntimeError("Async load failed")

    ref = LazyReference(
        entity_id="error_test",
        entity_cls=MockEntity,
        client=mock_client,
        async_loader_func=failing_async_loader
    )

    with pytest.raises(RuntimeError, match="Async load failed"):
        await ref.aget()

    # Second call should re-raise cached error
    with pytest.raises(RuntimeError, match="Async load failed"):
        await ref.aget()


class TestLazyReferenceWithRealEntity(unittest.TestCase):
    """Test LazyReference with Person entity class (mocked API)."""

    def test_lazy_reference_with_person_class(self):
        """Test LazyReference works with real Person entity class."""
        mock_client = MagicMock()

        ref = LazyReference(
            entity_id="person_123",
            entity_cls=Person,
            client=mock_client
        )

        self.assertEqual(ref.entity_id, "person_123")
        self.assertFalse(ref.is_loaded)
        # Don't call get() as it would try to make real API call
