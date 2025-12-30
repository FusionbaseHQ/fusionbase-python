"""Tests for entity type definitions and enums."""

import unittest

from fusionbase.types.entities import AddressComponentType
from fusionbase.types.entities import ConflictEventCategory
from fusionbase.types.entities import CyberSecurityEventCategory
from fusionbase.types.entities import EntityType
from fusionbase.types.entities import EventStatus
from fusionbase.types.entities import EventSubtype
from fusionbase.types.entities import FilterKey
from fusionbase.types.entities import LocationSubtype
from fusionbase.types.entities import NaturalEventCategory
from fusionbase.types.entities import OrganizationStatus
from fusionbase.types.entities import OrganizationStatusDetail
from fusionbase.types.entities import OrganizationSubtype
from fusionbase.types.entities import PersonSubtype
from fusionbase.types.entities import PublicationEventCategory


class TestEntityType(unittest.TestCase):
    """Test cases for EntityType enum."""

    def test_entity_type_values(self):
        """Test that EntityType has correct values."""
        self.assertEqual(EntityType.ORGANIZATION.value, "organization")
        self.assertEqual(EntityType.PERSON.value, "person")
        self.assertEqual(EntityType.LOCATION.value, "location")
        self.assertEqual(EntityType.EVENT.value, "event")
        self.assertEqual(EntityType.RELATION.value, "relation")
        self.assertEqual(EntityType.FEATURE.value, "feature")

    def test_entity_type_count(self):
        """Test that EntityType has expected number of members."""
        self.assertEqual(len(EntityType), 6)

    def test_entity_type_is_string_enum(self):
        """Test that EntityType values are strings."""
        for entity_type in EntityType:
            self.assertIsInstance(entity_type.value, str)

    def test_entity_type_comparison(self):
        """Test EntityType comparison with strings."""
        self.assertEqual(EntityType.ORGANIZATION, "organization")
        self.assertEqual(EntityType.PERSON, "person")

    def test_entity_type_from_string(self):
        """Test creating EntityType from string value."""
        org = EntityType("organization")
        self.assertEqual(org, EntityType.ORGANIZATION)

    def test_entity_type_iteration(self):
        """Test iterating over EntityType values."""
        types = list(EntityType)
        self.assertIn(EntityType.ORGANIZATION, types)
        self.assertIn(EntityType.PERSON, types)


class TestPersonSubtype(unittest.TestCase):
    """Test cases for PersonSubtype enum."""

    def test_person_subtype_values(self):
        """Test PersonSubtype values."""
        self.assertEqual(PersonSubtype.INDIVIDUAL.value, "INDIVIDUAL")
        self.assertEqual(PersonSubtype.ANY.value, "ANY")

    def test_person_subtype_count(self):
        """Test PersonSubtype has expected members."""
        self.assertEqual(len(PersonSubtype), 2)


class TestOrganizationSubtype(unittest.TestCase):
    """Test cases for OrganizationSubtype enum."""

    def test_organization_subtype_values(self):
        """Test OrganizationSubtype values."""
        self.assertEqual(OrganizationSubtype.CORPORATION.value, "CORPORATION")
        self.assertEqual(OrganizationSubtype.ANY.value, "ANY")
        self.assertEqual(OrganizationSubtype.BUSINESS.value, "BUSINESS")

    def test_organization_subtype_count(self):
        """Test OrganizationSubtype has expected members."""
        self.assertEqual(len(OrganizationSubtype), 3)


class TestOrganizationStatus(unittest.TestCase):
    """Test cases for OrganizationStatus enum."""

    def test_organization_status_common_values(self):
        """Test common OrganizationStatus values."""
        self.assertEqual(OrganizationStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(OrganizationStatus.INACTIVE.value, "INACTIVE")
        self.assertEqual(OrganizationStatus.UNKNOWN.value, "UNKNOWN")
        self.assertEqual(OrganizationStatus.LIQUIDATED.value, "LIQUIDATED")
        self.assertEqual(OrganizationStatus.DISSOLVED.value, "DISSOLVED")

    def test_organization_status_all_values(self):
        """Test all OrganizationStatus values exist."""
        expected_statuses = [
            "UNKNOWN", "INACTIVE", "ACTIVE", "LIQUIDATED", "DISSOLVED",
            "LIQUIDATION", "RECEIVER_ACTION", "CONVERTED_CLOSED",
            "VOLUNTARY_ARRANGEMENT", "INSOLVENCY_PROCEEDINGS",
            "IN_ADMINISTRATION", "CLOSED", "OPEN", "REGISTERED",
            "OPERATIONAL", "CLOSED_TEMPORARILY", "CLOSED_PERMANENTLY"
        ]
        actual_values = [s.value for s in OrganizationStatus]
        for expected in expected_statuses:
            self.assertIn(expected, actual_values)

    def test_organization_status_count(self):
        """Test OrganizationStatus has expected count."""
        self.assertEqual(len(OrganizationStatus), 17)


class TestOrganizationStatusDetail(unittest.TestCase):
    """Test cases for OrganizationStatusDetail enum."""

    def test_organization_status_detail_values(self):
        """Test OrganizationStatusDetail values."""
        self.assertEqual(
            OrganizationStatusDetail.ACTIVE_PROPOSAL_TO_STRIKE_OFF.value,
            "ACTIVE_PROPOSAL_TO_STRIKE_OFF"
        )
        self.assertEqual(
            OrganizationStatusDetail.CONVERTED_TO_PLC.value,
            "CONVERTED_TO_PLC"
        )

    def test_organization_status_detail_count(self):
        """Test OrganizationStatusDetail has expected count."""
        self.assertEqual(len(OrganizationStatusDetail), 6)


class TestLocationSubtype(unittest.TestCase):
    """Test cases for LocationSubtype enum."""

    def test_location_subtype_values(self):
        """Test LocationSubtype values."""
        self.assertEqual(LocationSubtype.POINT_OF_INTEREST.value, "POINT_OF_INTEREST")
        self.assertEqual(LocationSubtype.CITY_POSTAL_CODE.value, "CITY_POSTAL_CODE")
        self.assertEqual(LocationSubtype.COUNTRY.value, "COUNTRY")
        self.assertEqual(LocationSubtype.ANY.value, "ANY")

    def test_location_subtype_geographic_hierarchy(self):
        """Test location subtypes represent geographic hierarchy."""
        geographic_types = [
            LocationSubtype.COUNTRY,
            LocationSubtype.STATE,
            LocationSubtype.COUNTY,
            LocationSubtype.CITY_POSTAL_CODE,
            LocationSubtype.STREET,
            LocationSubtype.POINT_OF_INTEREST,
        ]
        for loc_type in geographic_types:
            self.assertIsInstance(loc_type.value, str)

    def test_location_subtype_count(self):
        """Test LocationSubtype has expected count."""
        self.assertEqual(len(LocationSubtype), 11)


class TestAddressComponentType(unittest.TestCase):
    """Test cases for AddressComponentType enum."""

    def test_address_component_type_values(self):
        """Test AddressComponentType values are lowercase."""
        self.assertEqual(AddressComponentType.COUNTRY.value, "country")
        self.assertEqual(AddressComponentType.CITY.value, "city")
        self.assertEqual(AddressComponentType.STREET.value, "street")
        self.assertEqual(AddressComponentType.POSTAL_CODE.value, "postal_code")

    def test_address_component_type_count(self):
        """Test AddressComponentType has expected count."""
        self.assertEqual(len(AddressComponentType), 7)


class TestEventSubtype(unittest.TestCase):
    """Test cases for EventSubtype enum."""

    def test_event_subtype_values(self):
        """Test EventSubtype values."""
        self.assertEqual(EventSubtype.NATURAL.value, "NATURAL")
        self.assertEqual(EventSubtype.CYBER_SECURITY.value, "CYBER_SECURITY")
        self.assertEqual(EventSubtype.CONFLICT.value, "CONFLICT")
        self.assertEqual(EventSubtype.PUBLICATION.value, "PUBLICATION")
        self.assertEqual(EventSubtype.ANY.value, "ANY")

    def test_event_subtype_count(self):
        """Test EventSubtype has expected count."""
        self.assertEqual(len(EventSubtype), 5)


class TestEventStatus(unittest.TestCase):
    """Test cases for EventStatus enum."""

    def test_event_status_values(self):
        """Test EventStatus values."""
        self.assertEqual(EventStatus.ISSUED.value, "ISSUED")
        self.assertEqual(EventStatus.ONGOING.value, "ONGOING")
        self.assertEqual(EventStatus.CANCELLED.value, "CANCELLED")
        self.assertEqual(EventStatus.FINISHED.value, "FINISHED")
        self.assertEqual(EventStatus.UNKNOWN.value, "UNKNOWN")

    def test_event_status_count(self):
        """Test EventStatus has expected count."""
        self.assertEqual(len(EventStatus), 5)


class TestPublicationEventCategory(unittest.TestCase):
    """Test cases for PublicationEventCategory enum."""

    def test_change_categories(self):
        """Test change-related publication categories."""
        change_categories = [
            PublicationEventCategory.NAME_CHANGE,
            PublicationEventCategory.LEGAL_FORM_CHANGE,
            PublicationEventCategory.ADDRESS_CHANGE,
            PublicationEventCategory.STATUS_CHANGE,
            PublicationEventCategory.CAPITAL_CHANGE,
        ]
        for cat in change_categories:
            self.assertIn("CHANGE", cat.value)

    def test_member_categories(self):
        """Test member-related publication categories."""
        member_categories = [
            PublicationEventCategory.MEMBER_ENTRY,
            PublicationEventCategory.MEMBER_EXIT_POSITION,
            PublicationEventCategory.MEMBER_NEW_POSITION,
            PublicationEventCategory.MEMBER_CHANGE,
        ]
        for cat in member_categories:
            self.assertIn("MEMBER", cat.value)

    def test_publication_event_category_count(self):
        """Test PublicationEventCategory has many members."""
        # Should have many categories for publications
        self.assertGreater(len(PublicationEventCategory), 20)


class TestNaturalEventCategory(unittest.TestCase):
    """Test cases for NaturalEventCategory enum."""

    def test_natural_event_category_values(self):
        """Test NaturalEventCategory values."""
        self.assertEqual(NaturalEventCategory.FLOODING.value, "FLOODING")
        self.assertEqual(NaturalEventCategory.EARTHQUAKE.value, "EARTHQUAKE")
        self.assertEqual(NaturalEventCategory.WILDFIRE.value, "WILDFIRE")
        self.assertEqual(NaturalEventCategory.HURRICANE.value, "HURRICANE")
        self.assertEqual(NaturalEventCategory.TSUNAMI.value, "TSUNAMI")

    def test_natural_event_category_all_disasters(self):
        """Test all natural disaster types exist."""
        disaster_types = [
            "FLOODING", "EARTHQUAKE", "WILDFIRE", "TORNADO",
            "HURRICANE", "TSUNAMI", "DROUGHT", "LANDSLIDE",
            "AVALANCHE", "VOLCANIC_ERUPTION"
        ]
        actual_values = [c.value for c in NaturalEventCategory]
        for disaster in disaster_types:
            self.assertIn(disaster, actual_values)

    def test_natural_event_category_count(self):
        """Test NaturalEventCategory has expected count."""
        self.assertEqual(len(NaturalEventCategory), 10)


class TestCyberSecurityEventCategory(unittest.TestCase):
    """Test cases for CyberSecurityEventCategory enum."""

    def test_cyber_security_category_values(self):
        """Test CyberSecurityEventCategory values."""
        self.assertEqual(CyberSecurityEventCategory.PHISHING.value, "PHISHING")
        self.assertEqual(CyberSecurityEventCategory.MALWARE.value, "MALWARE")
        self.assertEqual(CyberSecurityEventCategory.DDOS.value, "DDOS")
        self.assertEqual(CyberSecurityEventCategory.RANSOMWARE.value, "RANSOMWARE")

    def test_cyber_security_category_count(self):
        """Test CyberSecurityEventCategory has expected count."""
        self.assertEqual(len(CyberSecurityEventCategory), 8)


class TestConflictEventCategory(unittest.TestCase):
    """Test cases for ConflictEventCategory enum."""

    def test_conflict_event_category_values(self):
        """Test ConflictEventCategory values."""
        self.assertEqual(
            ConflictEventCategory.POLITICAL_VIOLENCE.value,
            "POLITICAL_VIOLENCE"
        )
        self.assertEqual(ConflictEventCategory.ARMED.value, "ARMED")
        self.assertEqual(ConflictEventCategory.CIVIL_UNREST.value, "CIVIL_UNREST")
        self.assertEqual(ConflictEventCategory.TERRORISM.value, "TERRORISM")

    def test_conflict_event_category_count(self):
        """Test ConflictEventCategory has expected count."""
        self.assertEqual(len(ConflictEventCategory), 4)


class TestFilterKey(unittest.TestCase):
    """Test cases for FilterKey enum."""

    def test_filter_key_values_lowercase(self):
        """Test FilterKey values are lowercase."""
        for key in FilterKey:
            self.assertEqual(key.value, key.value.lower())

    def test_filter_key_common_values(self):
        """Test common FilterKey values."""
        self.assertEqual(FilterKey.ACTIVE.value, "active")
        self.assertEqual(FilterKey.STATUS.value, "status")
        self.assertEqual(FilterKey.COUNTRY.value, "country")
        self.assertEqual(FilterKey.CITY.value, "city")

    def test_filter_key_address_components(self):
        """Test filter keys for address components."""
        address_keys = [
            FilterKey.COUNTRY,
            FilterKey.STATE,
            FilterKey.COUNTY,
            FilterKey.CITY,
            FilterKey.POSTAL_CODE,
            FilterKey.STREET,
            FilterKey.HOUSE_NUMBER,
        ]
        for key in address_keys:
            self.assertIsInstance(key.value, str)


class TestEnumInteroperability(unittest.TestCase):
    """Test enum interoperability and usage patterns."""

    def test_enum_string_comparison(self):
        """Test enums can be compared to strings."""
        self.assertTrue(EntityType.ORGANIZATION == "organization")
        self.assertTrue(EventStatus.ONGOING != EventStatus.FINISHED)

    def test_enum_in_dict_key(self):
        """Test enums can be used as dictionary keys."""
        data = {
            EntityType.ORGANIZATION: "org_data",
            EntityType.PERSON: "person_data",
        }
        self.assertEqual(data[EntityType.ORGANIZATION], "org_data")

    def test_enum_json_serializable(self):
        """Test enum values are JSON serializable."""
        import json

        data = {
            "entity_type": EntityType.ORGANIZATION.value,
            "status": OrganizationStatus.ACTIVE.value,
        }
        json_str = json.dumps(data)
        parsed = json.loads(json_str)

        self.assertEqual(parsed["entity_type"], "organization")
        self.assertEqual(parsed["status"], "ACTIVE")

    def test_enum_membership_check(self):
        """Test checking if value is valid enum member."""
        valid_types = [e.value for e in EntityType]

        self.assertIn("organization", valid_types)
        self.assertIn("person", valid_types)
        self.assertNotIn("invalid_type", valid_types)

    def test_enum_name_vs_value(self):
        """Test difference between enum name and value."""
        self.assertEqual(EntityType.ORGANIZATION.name, "ORGANIZATION")
        self.assertEqual(EntityType.ORGANIZATION.value, "organization")

        # Name is uppercase, value is lowercase for EntityType
        self.assertNotEqual(
            EntityType.ORGANIZATION.name,
            EntityType.ORGANIZATION.value
        )
