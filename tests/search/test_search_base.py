"""Tests for base search classes."""

import unittest
from unittest.mock import MagicMock

from fusionbase.entities.base import Entity
from fusionbase.entities.person import Person
from fusionbase.search.base import BaseSearch
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult


class TestSearchParams(unittest.TestCase):
    """Test cases for SearchParams base class."""

    def test_search_params_creation(self):
        """Test creating SearchParams instance."""
        params = SearchParams()

        self.assertIsNotNone(params)

    def test_search_params_ignores_extra_fields(self):
        """Test that extra fields are ignored."""
        # SearchParams has extra="ignore" in model_config
        params = SearchParams.model_validate({
            "unknown_field": "value",
            "another_unknown": 123
        })

        self.assertIsNotNone(params)
        self.assertFalse(hasattr(params, "unknown_field"))

    def test_search_params_model_dump(self):
        """Test SearchParams serialization."""
        params = SearchParams()

        dumped = params.model_dump()

        self.assertIsInstance(dumped, dict)

    def test_search_params_subclass(self):
        """Test creating SearchParams subclass."""
        class CustomSearchParams(SearchParams):
            q: str = ""
            limit: int = 10
            skip: int = 0

        params = CustomSearchParams(q="test query", limit=20)

        self.assertEqual(params.q, "test query")
        self.assertEqual(params.limit, 20)
        self.assertEqual(params.skip, 0)

    def test_search_params_subclass_model_dump(self):
        """Test subclass serialization includes all fields."""
        class CustomSearchParams(SearchParams):
            q: str = ""
            custom_field: str = "default"

        params = CustomSearchParams(q="search", custom_field="custom")
        dumped = params.model_dump()

        self.assertEqual(dumped["q"], "search")
        self.assertEqual(dumped["custom_field"], "custom")


class TestSearchResult(unittest.TestCase):
    """Test cases for SearchResult class."""

    def test_search_result_creation(self):
        """Test creating SearchResult instance."""
        result = SearchResult(
            items=["a", "b", "c"],
            total=3,
            limit=10,
            skip=0
        )

        self.assertEqual(len(result.items), 3)
        self.assertEqual(result.total, 3)
        self.assertEqual(result.limit, 10)
        self.assertEqual(result.skip, 0)

    def test_search_result_with_params(self):
        """Test SearchResult with params."""
        params = SearchParams()
        result = SearchResult(
            items=[],
            total=0,
            limit=10,
            skip=0,
            params=params
        )

        self.assertEqual(result.params, params)

    def test_search_result_len(self):
        """Test SearchResult __len__ method."""
        result = SearchResult(
            items=[1, 2, 3, 4, 5],
            total=100,
            limit=5,
            skip=0
        )

        self.assertEqual(len(result), 5)
        # len returns items count, not total
        self.assertNotEqual(len(result), result.total)

    def test_search_result_getitem(self):
        """Test SearchResult __getitem__ method."""
        items = ["first", "second", "third"]
        result = SearchResult(
            items=items,
            total=3,
            limit=10,
            skip=0
        )

        self.assertEqual(result[0], "first")
        self.assertEqual(result[1], "second")
        self.assertEqual(result[2], "third")

    def test_search_result_getitem_negative_index(self):
        """Test SearchResult with negative index."""
        items = ["a", "b", "c"]
        result = SearchResult(
            items=items,
            total=3,
            limit=10,
            skip=0
        )

        self.assertEqual(result[-1], "c")
        self.assertEqual(result[-2], "b")

    def test_search_result_getitem_slice(self):
        """Test SearchResult with slice."""
        items = [1, 2, 3, 4, 5]
        result = SearchResult(
            items=items,
            total=5,
            limit=10,
            skip=0
        )

        self.assertEqual(result[1:3], [2, 3])
        self.assertEqual(result[:2], [1, 2])
        self.assertEqual(result[3:], [4, 5])

    def test_search_result_iter(self):
        """Test SearchResult __iter__ method."""
        items = ["x", "y", "z"]
        result = SearchResult(
            items=items,
            total=3,
            limit=10,
            skip=0
        )

        collected = list(result)

        self.assertEqual(collected, ["x", "y", "z"])

    def test_search_result_in_for_loop(self):
        """Test using SearchResult in for loop."""
        items = [10, 20, 30]
        result = SearchResult(
            items=items,
            total=3,
            limit=10,
            skip=0
        )

        total = 0
        for item in result:
            total += item

        self.assertEqual(total, 60)

    def test_search_result_empty(self):
        """Test empty SearchResult."""
        result = SearchResult(
            items=[],
            total=0,
            limit=10,
            skip=0
        )

        self.assertEqual(len(result), 0)
        self.assertEqual(list(result), [])

    def test_search_result_pagination_info(self):
        """Test SearchResult pagination information."""
        # Simulating second page of results
        result = SearchResult(
            items=["item11", "item12"],
            total=25,  # Total available
            limit=10,
            skip=10  # Skipped first 10
        )

        self.assertEqual(result.skip, 10)
        self.assertEqual(result.limit, 10)
        self.assertEqual(result.total, 25)
        self.assertEqual(len(result), 2)  # Items on this page


class TestBaseSearch(unittest.TestCase):
    """Test cases for BaseSearch class."""

    def test_base_search_initialization(self):
        """Test BaseSearch initialization."""
        mock_client = MagicMock()

        search = BaseSearch(client=mock_client, entity_class=Person)

        self.assertEqual(search.client, mock_client)
        self.assertEqual(search.entity_class, Person)

    def test_base_search_stores_client(self):
        """Test that BaseSearch stores client reference."""
        mock_client = MagicMock()
        mock_client.name = "test_client"

        search = BaseSearch(client=mock_client, entity_class=Entity)

        self.assertEqual(search.client.name, "test_client")

    def test_base_search_stores_entity_class(self):
        """Test that BaseSearch stores entity class."""
        mock_client = MagicMock()

        search = BaseSearch(client=mock_client, entity_class=Person)

        self.assertEqual(search.entity_class, Person)
        self.assertEqual(search.entity_class.__name__, "Person")

    def test_base_search_with_different_entity_classes(self):
        """Test BaseSearch with different entity classes."""
        from fusionbase.entities.location import Location
        from fusionbase.entities.organization import Organization

        mock_client = MagicMock()

        person_search = BaseSearch(client=mock_client, entity_class=Person)
        org_search = BaseSearch(client=mock_client, entity_class=Organization)
        loc_search = BaseSearch(client=mock_client, entity_class=Location)

        self.assertEqual(person_search.entity_class, Person)
        self.assertEqual(org_search.entity_class, Organization)
        self.assertEqual(loc_search.entity_class, Location)


class TestSearchResultGenericType(unittest.TestCase):
    """Test SearchResult with generic type annotations."""

    def test_search_result_with_entity_items(self):
        """Test SearchResult containing entity objects."""
        mock_entities = [
            MagicMock(fb_entity_id="1"),
            MagicMock(fb_entity_id="2"),
        ]

        result = SearchResult(
            items=mock_entities,
            total=2,
            limit=10,
            skip=0
        )

        self.assertEqual(result[0].fb_entity_id, "1")
        self.assertEqual(result[1].fb_entity_id, "2")

    def test_search_result_with_lazy_references(self):
        """Test SearchResult containing LazyReference objects."""
        from fusionbase.entities.lazy_reference import LazyReference

        mock_client = MagicMock()
        refs = [
            LazyReference("id1", Person, mock_client),
            LazyReference("id2", Person, mock_client),
        ]

        result = SearchResult(
            items=refs,
            total=2,
            limit=10,
            skip=0
        )

        self.assertEqual(result[0].entity_id, "id1")
        self.assertEqual(result[1].entity_id, "id2")


class TestSearchResultUsagePatterns(unittest.TestCase):
    """Test common usage patterns with SearchResult."""

    def test_check_if_has_results(self):
        """Test checking if search has results."""
        empty_result = SearchResult(items=[], total=0, limit=10, skip=0)
        has_results = SearchResult(items=["x"], total=1, limit=10, skip=0)

        self.assertFalse(bool(len(empty_result)))
        self.assertTrue(bool(len(has_results)))

    def test_check_if_more_results_available(self):
        """Test checking if more results are available."""
        result = SearchResult(
            items=["a", "b"],
            total=10,  # 10 total, but only 2 returned
            limit=2,
            skip=0
        )

        has_more = result.total > (result.skip + len(result))

        self.assertTrue(has_more)

    def test_calculate_next_skip(self):
        """Test calculating skip for next page."""
        result = SearchResult(
            items=list(range(10)),
            total=50,
            limit=10,
            skip=0
        )

        next_skip = result.skip + len(result)

        self.assertEqual(next_skip, 10)

    def test_iterate_with_enumerate(self):
        """Test iterating with enumerate."""
        result = SearchResult(
            items=["a", "b", "c"],
            total=3,
            limit=10,
            skip=0
        )

        indexed = list(enumerate(result))

        self.assertEqual(indexed, [(0, "a"), (1, "b"), (2, "c")])

    def test_list_comprehension(self):
        """Test using list comprehension with SearchResult."""
        mock_items = [
            MagicMock(name="Item1"),
            MagicMock(name="Item2"),
            MagicMock(name="Item3"),
        ]
        result = SearchResult(
            items=mock_items,
            total=3,
            limit=10,
            skip=0
        )

        names = [item.name for item in result]

        self.assertEqual(len(names), 3)
