"""Tests for Source model."""

import unittest

from fusionbase.data.source import Source


class TestSource(unittest.TestCase):
    """Test cases for Source model."""

    def test_source_initialization(self):
        """Test creating a Source object."""
        # Create source with id
        source1 = Source(id="source_id_123")
        self.assertEqual(source1.id, "source_id_123")
        self.assertIsNone(source1.internal_id)  # Use internal_id instead of _id
        self.assertEqual(source1.source_id, "source_id_123")

        # Create source with _id
        source2 = Source(_id="internal_id_456")
        self.assertEqual(source2.internal_id,
                         "internal_id_456")  # Use internal_id instead of _id
        self.assertIsNone(source2.id)
        self.assertEqual(source2.source_id, "internal_id_456")

        # Create source with both id and _id
        source3 = Source(id="source_id_123", _id="internal_id_456")
        self.assertEqual(source3.id, "source_id_123")
        self.assertEqual(source3.internal_id,
                         "internal_id_456")  # Use internal_id instead of _id
        self.assertEqual(source3.source_id,
                         "source_id_123")  # id takes precedence

        # Create source with service_specific
        source4 = Source(id="source_id_123", service_specific={"key": "value"})
        self.assertEqual(source4.service_specific["key"], "value")

        # Create source with uri
        source5 = Source(id="source_id_123", uri="https://example.com")
        self.assertEqual(source5.uri, "https://example.com")

    def test_source_from_api_response(self):
        """Test creating a Source from typical API response data."""
        # Sample API response data
        api_data = {
            "_id": "data_sources/2685259",
            "service_specific": {
                "uri": "https://fusionbase.com"
            }
        }

        # Create source from API data
        source = Source.model_validate(api_data)

        # Validate
        self.assertEqual(
            source.internal_id,
            "data_sources/2685259")  # Use internal_id instead of _id
        self.assertIsNone(source.id)
        self.assertEqual(source.source_id, "data_sources/2685259")
        self.assertEqual(source.service_specific["uri"],
                         "https://fusionbase.com")
