"""Tests for search utility functions."""

import json
import unittest
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from fusionbase.entities.lazy_reference import LazyReference
from fusionbase.entities.organization import Organization
from fusionbase.entities.person import Person
from fusionbase.exceptions import APIError
from fusionbase.search.base import SearchParams
from fusionbase.search.base import SearchResult
from fusionbase.utils.search_utils import make_search_request
from fusionbase.utils.search_utils import make_search_request_async
from fusionbase.utils.search_utils import prepare_search_params
from fusionbase.utils.search_utils import process_search_results


class TestSearchParams(SearchParams):
    """Test search params class."""
    q: str = ""
    limit: int = 10
    skip: int = 0
    filters: dict = None


class TestPrepareSearchParams(unittest.TestCase):
    """Test cases for prepare_search_params function."""

    def test_prepare_with_kwargs(self):
        """Test prepare_search_params with keyword arguments."""
        params, query_dict = prepare_search_params(param_class=TestSearchParams,
                                                   q="test query",
                                                   limit=20)

        self.assertIsInstance(params, TestSearchParams)
        self.assertEqual(params.q, "test query")
        self.assertEqual(params.limit, 20)
        self.assertEqual(query_dict["q"], "test query")
        self.assertEqual(query_dict["limit"], 20)

    def test_prepare_with_params_object(self):
        """Test prepare_search_params with params object."""
        input_params = TestSearchParams(q="search term", skip=5)

        params, query_dict = prepare_search_params(params=input_params,
                                                   param_class=TestSearchParams)

        self.assertEqual(params.q, "search term")
        self.assertEqual(params.skip, 5)
        self.assertEqual(query_dict["q"], "search term")
        self.assertEqual(query_dict["skip"], 5)

    def test_prepare_with_no_params_creates_default(self):
        """Test prepare_search_params creates default when no params."""
        params, query_dict = prepare_search_params(param_class=TestSearchParams,
                                                   require_query=False)

        self.assertIsInstance(params, TestSearchParams)
        self.assertEqual(params.q, "")

    def test_prepare_raises_when_query_required_but_missing(self):
        """Test ValueError when query is required but not provided."""
        with self.assertRaises(ValueError) as context:
            prepare_search_params(param_class=TestSearchParams,
                                  require_query=True)

        self.assertIn("query", str(context.exception).lower())

    def test_prepare_does_not_raise_when_query_not_required(self):
        """Test no error when query not required."""
        params, query_dict = prepare_search_params(param_class=TestSearchParams,
                                                   require_query=False)

        self.assertIsInstance(params, TestSearchParams)

    def test_prepare_excludes_none_values(self):
        """Test that None values are excluded from query dict."""
        params, query_dict = prepare_search_params(param_class=TestSearchParams,
                                                   q="test",
                                                   require_query=True)

        # filters is None by default, should be excluded
        self.assertNotIn("filters", query_dict)

    def test_prepare_converts_filters_to_json(self):
        """Test that filters dict is converted to JSON string."""
        filters = {"status": "active", "country": "DE"}
        input_params = TestSearchParams(q="test", filters=filters)

        params, query_dict = prepare_search_params(params=input_params,
                                                   param_class=TestSearchParams)

        self.assertIsInstance(query_dict["filters"], str)
        parsed_filters = json.loads(query_dict["filters"])
        self.assertEqual(parsed_filters["status"], "active")
        self.assertEqual(parsed_filters["country"], "DE")


class TestProcessSearchResults(unittest.TestCase):
    """Test cases for process_search_results function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.params = TestSearchParams(q="test", limit=10, skip=0)

    def test_process_empty_results(self):
        """Test processing response with no results key."""
        response_data = {}

        result = process_search_results(response_data=response_data,
                                        entity_class=Person,
                                        client=self.mock_client,
                                        params=self.params)

        self.assertIsInstance(result, SearchResult)
        self.assertEqual(len(result.items), 0)
        self.assertEqual(result.total, 0)

    def test_process_empty_results_list(self):
        """Test processing response with empty results list."""
        response_data = {"results": [], "total": 0}

        result = process_search_results(response_data=response_data,
                                        entity_class=Person,
                                        client=self.mock_client,
                                        params=self.params)

        self.assertEqual(len(result.items), 0)
        self.assertEqual(result.total, 0)

    def test_process_results_creates_lazy_references(self):
        """Test that results are converted to LazyReference objects."""
        response_data = {
            "results": [
                {
                    "entity": {
                        "fb_entity_id": "person_123"
                    }
                },
                {
                    "entity": {
                        "fb_entity_id": "person_456"
                    }
                },
            ],
            "total": 2
        }

        result = process_search_results(response_data=response_data,
                                        entity_class=Person,
                                        client=self.mock_client,
                                        params=self.params)

        self.assertEqual(len(result.items), 2)
        self.assertIsInstance(result.items[0], LazyReference)
        self.assertIsInstance(result.items[1], LazyReference)
        self.assertEqual(result.items[0].entity_id, "person_123")
        self.assertEqual(result.items[1].entity_id, "person_456")

    def test_process_results_preserves_total(self):
        """Test that total count is preserved from response."""
        response_data = {
            "results": [{
                "entity": {
                    "fb_entity_id": "org_1"
                }
            },],
            "total": 100  # More results available than returned
        }

        result = process_search_results(response_data=response_data,
                                        entity_class=Organization,
                                        client=self.mock_client,
                                        params=self.params)

        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.total, 100)

    def test_process_results_uses_params_limit_skip(self):
        """Test that limit and skip are taken from params."""
        params = TestSearchParams(q="test", limit=25, skip=50)
        response_data = {"results": [], "total": 0}

        result = process_search_results(response_data=response_data,
                                        entity_class=Person,
                                        client=self.mock_client,
                                        params=params)

        self.assertEqual(result.limit, 25)
        self.assertEqual(result.skip, 50)

    def test_process_skips_invalid_results(self):
        """Test that invalid result entries are skipped."""
        response_data = {
            "results": [
                {
                    "entity": {
                        "fb_entity_id": "valid_1"
                    }
                },
                {
                    "not_entity": "invalid"
                },  # Missing 'entity' key
                "not_a_dict",  # Not a dict
                {
                    "entity": {}
                },  # Missing fb_entity_id
                {
                    "entity": {
                        "fb_entity_id": "valid_2"
                    }
                },
            ],
            "total": 5
        }

        result = process_search_results(response_data=response_data,
                                        entity_class=Person,
                                        client=self.mock_client,
                                        params=self.params)

        # Only valid entries should be processed
        self.assertEqual(len(result.items), 2)
        self.assertEqual(result.items[0].entity_id, "valid_1")
        self.assertEqual(result.items[1].entity_id, "valid_2")

    def test_process_preserves_params_in_result(self):
        """Test that params are preserved in result."""
        result = process_search_results(response_data={"results": []},
                                        entity_class=Person,
                                        client=self.mock_client,
                                        params=self.params)

        self.assertEqual(result.params, self.params)


class TestMakeSearchRequest(unittest.TestCase):
    """Test cases for make_search_request function."""

    def test_make_request_with_request_method(self):
        """Test make_search_request using client.request method."""
        mock_client = MagicMock()
        mock_client.request.return_value = {"results": [], "total": 0}

        result = make_search_request(client=mock_client,
                                     endpoint="search/persons",
                                     params={"q": "test"})

        mock_client.request.assert_called_once_with("GET",
                                                    "search/persons",
                                                    params={"q": "test"})
        self.assertEqual(result, {"results": [], "total": 0})

    def test_make_request_with_http_client_fallback(self):
        """Test make_search_request fallback to http_client."""
        mock_client = MagicMock(spec=[])  # No 'request' attribute
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [], "total": 0}
        mock_client.http_client = MagicMock()
        mock_client.http_client.get.return_value = mock_response

        result = make_search_request(client=mock_client,
                                     endpoint="search/organizations",
                                     params={"q": "company"})

        mock_client.http_client.get.assert_called_once()
        self.assertEqual(result, {"results": [], "total": 0})

    def test_make_request_raises_api_error_on_failure(self):
        """Test make_search_request raises APIError on failure."""
        mock_client = MagicMock()
        mock_client.request.side_effect = OSError("Connection failed")

        with self.assertRaises(APIError) as context:
            make_search_request(client=mock_client,
                                endpoint="search/persons",
                                params={"q": "test"})

        self.assertIn("Search failed", str(context.exception))
        self.assertEqual(context.exception.status_code, 500)


@pytest.mark.asyncio
async def test_make_search_request_async_with_arequest():
    """Test async search with arequest method."""
    mock_client = MagicMock()
    mock_client.arequest = AsyncMock(return_value={"results": [], "total": 0})

    result = await make_search_request_async(client=mock_client,
                                             endpoint="search/persons",
                                             params={"q": "async test"})

    mock_client.arequest.assert_called_once_with("GET",
                                                 "search/persons",
                                                 params={"q": "async test"})
    assert result == {"results": [], "total": 0}


@pytest.mark.asyncio
async def test_make_search_request_async_with_aget():
    """Test async search with aget method."""
    mock_client = MagicMock(spec=[])
    mock_client.aget = AsyncMock(return_value={"results": [], "total": 0})

    result = await make_search_request_async(client=mock_client,
                                             endpoint="search/locations",
                                             params={"q": "Berlin"})

    mock_client.aget.assert_called_once()
    assert result == {"results": [], "total": 0}


@pytest.mark.asyncio
async def test_make_search_request_async_with_async_http_client():
    """Test async search with _async_http_client."""
    mock_client = MagicMock(spec=[])
    mock_response = MagicMock()
    mock_response.json.return_value = {"results": [], "total": 0}
    mock_client._async_http_client = MagicMock()
    mock_client._async_http_client.get = AsyncMock(return_value=mock_response)

    result = await make_search_request_async(client=mock_client,
                                             endpoint="search/organizations",
                                             params={"q": "GmbH"})

    mock_client._async_http_client.get.assert_called_once()
    assert result == {"results": [], "total": 0}


@pytest.mark.asyncio
async def test_make_search_request_async_raises_when_no_method():
    """Test async search raises APIError when no async method available."""
    mock_client = MagicMock(spec=[])  # No async methods

    with pytest.raises(APIError) as exc_info:
        await make_search_request_async(client=mock_client,
                                        endpoint="search/persons",
                                        params={"q": "test"})

    assert "No suitable async method found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_make_search_request_async_raises_on_io_error():
    """Test async search raises APIError on IO error."""
    mock_client = MagicMock()
    mock_client.arequest = AsyncMock(side_effect=IOError("Network error"))

    with pytest.raises(APIError) as exc_info:
        await make_search_request_async(client=mock_client,
                                        endpoint="search/persons",
                                        params={"q": "test"})

    assert "Async search failed" in str(exc_info.value)


class TestSearchUtilsIntegration(unittest.TestCase):
    """Integration tests for search utilities working together."""

    def test_full_search_flow(self):
        """Test complete search flow with all utilities."""
        mock_client = MagicMock()
        mock_client.request.return_value = {
            "results": [
                {
                    "entity": {
                        "fb_entity_id": "person_1"
                    }
                },
                {
                    "entity": {
                        "fb_entity_id": "person_2"
                    }
                },
            ],
            "total": 2
        }

        # Prepare params
        params, query_dict = prepare_search_params(param_class=TestSearchParams,
                                                   q="John",
                                                   limit=10)

        # Make request
        response = make_search_request(client=mock_client,
                                       endpoint="search/persons",
                                       params=query_dict)

        # Process results
        result = process_search_results(response_data=response,
                                        entity_class=Person,
                                        client=mock_client,
                                        params=params)

        self.assertEqual(len(result), 2)
        self.assertEqual(result.total, 2)
        self.assertIsInstance(result[0], LazyReference)
