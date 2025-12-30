"""Tests for Fusion search functionality."""

import unittest
from unittest.mock import patch

import pytest
from conftest import get_api_key

from fusionbase import Fusionbase


class TestFusionSearch(unittest.TestCase):
    """Test cases for Fusion search functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Use API key from environment or skip tests if not available
        self.api_key = get_api_key()
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Create client with real API key
        self.client = Fusionbase(api_key=self.api_key)

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "client"):
            self.client.close()

    def test_fusion_search_mock(self):
        """Test searching with fusion search using a mocked response."""
        # Create a mock response
        mock_response = {
            "knowledge_graph": {
                "intent": "RELATION",
                "from_entity_type": "ORGANIZATION",
                "from_entity_id": "546bb06ee922e3b09dc50c50068044a3",
                "relation_id": "67533702",
                "relation_parameters": {}
            },
            "results": {
                "persons": [],
                "organizations": [{
                    "entity_type": "ORGANIZATION",
                    "entity": {
                        "fb_entity_id":
                            "546bb06ee922e3b09dc50c50068044a3",
                        "status":
                            "ACTIVE",
                        "display_address":
                            "Aidenbachstraße 140, 81479 Munich, Germany",
                        "registration_authority_entity_name":
                            "München HRB 137024",
                        "display_name":
                            "Health Care Insurance Versicherungsservice GmbH, Zweigniederlassung München",
                        "name":
                            "Health Care Insurance Versicherungsservice GmbH, Zweigniederlassung München"
                    },
                    "score": 10.070869
                }],
                "locations": [],
                "relations": [{
                    "entity": {
                        "id": "64395606",
                        "key": "64395606",
                        "name": {
                            "en":
                                "Multiscale Global and Local Statistical Indicators for Locations",
                            "de":
                                "Standortbezogene Globale und Lokale Statistische Indikatoren"
                        }
                    },
                    "score": 0.649,
                    "distance": 0.350,
                    "entity_type": "RELATION"
                }],
                "streams": [{
                    "linked_data_id":
                        "",
                    "linked_context_id":
                        "55012638",
                    "linked_data_value":
                        "USA: ACS1 - Imputation of Direct-purchase Health Insurance",
                    "entity": {
                        "key": "55012638",
                        "name": {
                            "en":
                                "USA: ACS1 - Imputation of Direct-purchase Health Insurance"
                        },
                        "source": {
                            "name": "United States Census Bureau"
                        }
                    },
                    "score":
                        0.711,
                    "entity_type":
                        "STREAM"
                }],
                "services": []
            },
            "ranks": ["STREAM", "RELATION", "ORGANIZATION"]
        }

        # Create a mock for the client.request method
        with patch.object(self.client, 'request',
                          return_value=mock_response) as mock_request:
            # Search with fusion search - q is now a required positional argument
            results = self.client.search.fusion.search("health insurance")

            # Verify the mock was called
            mock_request.assert_called_once()

            # Verify search results
            self.assertIsNotNone(results.knowledge_graph)
            self.assertEqual(results.knowledge_graph.intent, "RELATION")

            # Check organizations
            self.assertEqual(len(results.results["organizations"]), 1)
            self.assertEqual(
                results.results["organizations"][0]["entity"]["fb_entity_id"],
                "546bb06ee922e3b09dc50c50068044a3")

            # Check streams
            self.assertEqual(len(results.results["streams"]), 1)
            self.assertEqual(results.results["streams"][0]["entity"]["key"],
                             "55012638")

            # Check relations
            self.assertEqual(len(results.results["relations"]), 1)
            self.assertEqual(results.results["relations"][0]["entity"]["id"],
                             "64395606")

            # Check ranks
            self.assertEqual(results.ranks[0], "STREAM")

            # Check that the request was made with the correct parameters
            args, kwargs = mock_request.call_args
            self.assertEqual(args[0], "GET")
            self.assertEqual(args[1], "search/fusion")
            self.assertEqual(
                kwargs.get("params", {}).get("q"), "health insurance")

    def test_real_api_fusion_search(self):
        """Test searching with fusion search using the real API."""
        if not self.api_key:
            self.skipTest("FUSIONBASE_API_KEY environment variable not set")

        # Search for a generic term - q is now a required positional argument
        results = self.client.search.fusion.search("health insurance")

        # Verify we have results structure
        self.assertIsNotNone(results)
        self.assertIsNotNone(results.results)
        self.assertIsInstance(results.ranks, list)

        # Check that at least one entity type has results
        has_results = any(
            len(results.results.get(entity_type, [])) > 0 for entity_type in [
                "organizations", "persons", "locations", "relations", "streams",
                "services"
            ])
        self.assertTrue(has_results, "No results found in any entity type")


@pytest.mark.asyncio
async def test_fusion_search_async():
    """Test async fusion searching with real API."""
    api_key = get_api_key()
    if not api_key:
        pytest.skip("FUSIONBASE_API_KEY environment variable not set")

    # Create client with real API key
    client = Fusionbase(api_key=api_key)

    try:
        try:
            # Use async methods directly on the client
            results = await client.search.fusion.asearch("health insurance")

            # Verify we have results structure
            assert results is not None
            assert results.results is not None
            assert isinstance(results.ranks, list)

            # Check that at least one entity type has results
            has_results = any(
                len(results.results.get(entity_type, [])) > 0
                for entity_type in [
                    "organizations", "persons", "locations", "relations",
                    "streams", "services"
                ])
            assert has_results, "No results found in any entity type"

        except Exception as e:
            pytest.fail(f"Async search failed with error: {e}")

    finally:
        # Close client (close both sync and async resources)
        await client.aclose()
        client.close()
