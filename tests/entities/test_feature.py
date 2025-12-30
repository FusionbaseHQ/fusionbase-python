"""Tests for the Feature entity."""

import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fusionbase.entities.feature import Feature
from fusionbase.types.entities import EntityType


class TestFeature(unittest.TestCase):
    """Test cases for Feature entity."""

    def test_feature_entity_type(self):
        """Test that Feature has correct entity type."""
        self.assertEqual(Feature.entity_type, EntityType.FEATURE)

    def test_feature_default_subtype(self):
        """Test that Feature has correct default subtype."""
        feature = Feature(fb_entity_id="test_id", fb_entity_version="v1")
        self.assertEqual(feature.entity_subtype, "FEATURE")

    def test_feature_with_dict_value(self):
        """Test creating Feature with dictionary value."""
        feature = Feature(fb_entity_id="test_id",
                          fb_entity_version="v1",
                          value={
                              "key": "value",
                              "number": 42
                          })
        self.assertEqual(feature.value, {"key": "value", "number": 42})

    def test_feature_with_list_value(self):
        """Test creating Feature with list value."""
        feature = Feature(fb_entity_id="test_id",
                          fb_entity_version="v1",
                          value=["item1", "item2", "item3"])
        self.assertEqual(feature.value, ["item1", "item2", "item3"])

    def test_feature_with_string_value(self):
        """Test creating Feature with string value."""
        feature = Feature(fb_entity_id="test_id",
                          fb_entity_version="v1",
                          value="simple string value")
        self.assertEqual(feature.value, "simple string value")

    def test_feature_with_none_value(self):
        """Test creating Feature with None value."""
        feature = Feature(fb_entity_id="test_id",
                          fb_entity_version="v1",
                          value=None)
        self.assertIsNone(feature.value)

    def test_feature_model_validation(self):
        """Test Feature model validation from dict."""
        data = {
            "fb_entity_id": "feature_123",
            "fb_entity_version": "v2",
            "value": {
                "stat": 99.5
            }
        }
        feature = Feature.model_validate(data)
        self.assertEqual(feature.fb_entity_id, "feature_123")
        self.assertEqual(feature.fb_entity_version, "v2")
        self.assertEqual(feature.value, {"stat": 99.5})

    def test_feature_from_id_mock(self):
        """Test Feature._from_id with mocked API response."""
        mock_client = MagicMock()
        mock_response_data = {
            "fb_entity_id": "feature_abc",
            "fb_entity_version": "v1",
            "value": {
                "indicator": "high"
            }
        }

        with patch('fusionbase.utils.api_utils.make_entity_request'
                  ) as mock_request:
            mock_request.return_value = mock_response_data
            feature = Feature._from_id(mock_client, "feature_abc")

            self.assertEqual(feature.fb_entity_id, "feature_abc")
            self.assertEqual(feature.value, {"indicator": "high"})
            mock_request.assert_called_once_with(mock_client, "feature",
                                                 "feature_abc")


@pytest.mark.asyncio
async def test_feature_afrom_id_mock():
    """Test Feature._afrom_id with mocked API response."""
    mock_client = MagicMock()
    mock_response_data = {
        "fb_entity_id": "feature_async",
        "fb_entity_version": "v1",
        "value": ["async", "data"]
    }

    with patch('fusionbase.utils.api_utils.make_entity_request_async'
              ) as mock_request:
        mock_request.return_value = mock_response_data
        feature = await Feature._afrom_id(mock_client, "feature_async")

        assert feature.fb_entity_id == "feature_async"
        assert feature.value == ["async", "data"]
        mock_request.assert_called_once_with(mock_client, "feature",
                                             "feature_async")


class TestFeatureComplexValues(unittest.TestCase):
    """Test cases for Feature with complex value structures."""

    def test_feature_nested_dict_value(self):
        """Test Feature with deeply nested dictionary."""
        complex_value = {
            "level1": {
                "level2": {
                    "level3": "deep_value"
                }
            },
            "array": [1, 2, 3]
        }
        feature = Feature(fb_entity_id="complex_id",
                          fb_entity_version="v1",
                          value=complex_value)
        self.assertEqual(feature.value["level1"]["level2"]["level3"],
                         "deep_value")

    def test_feature_mixed_array_value(self):
        """Test Feature with mixed type array."""
        mixed_value = [1, "string", {"key": "value"}, [1, 2, 3]]
        feature = Feature(fb_entity_id="mixed_id",
                          fb_entity_version="v1",
                          value=mixed_value)
        self.assertEqual(len(feature.value), 4)
        self.assertEqual(feature.value[2], {"key": "value"})
