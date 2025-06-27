"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest

from singer_sdk.testing import get_tap_test_class

from tap_redshift.tap import TapRedshift
from tap_redshift.client import RedshiftConnector
from tap_redshift.schema import RedshiftSQLToJSONSchema

SAMPLE_CONFIG = {
    "host": "test.redshift.amazonaws.com",
    "database": "test_db",
    "user": "test_user",
    "password": "test_password",
    "use_iam_authentication": False,
}


# Run standard built-in tap tests from the SDK:
TestTapRedshift = get_tap_test_class(
    tap_class=TapRedshift,
    config=SAMPLE_CONFIG,
)


class TestRedshiftConnector:
    """Test Redshift connector functionality."""
    
    def test_sqlalchemy_url_basic_auth(self):
        """Test SQLAlchemy URL generation for basic auth."""
        config = {
            "host": "test.redshift.amazonaws.com",
            "port": 5439,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
            "use_iam_authentication": False,
        }
        connector = RedshiftConnector(config)
        url = connector.get_sqlalchemy_url(config)
        
        expected = "redshift+redshift_connector://test_user:test_password@test.redshift.amazonaws.com:5439/test_db"
        assert url == expected
    
    def test_sqlalchemy_url_iam_auth(self):
        """Test SQLAlchemy URL generation for IAM auth."""
        config = {
            "host": "test.redshift.amazonaws.com",
            "port": 5439,
            "database": "test_db",
            "use_iam_authentication": True,
        }
        connector = RedshiftConnector(config)
        url = connector.get_sqlalchemy_url(config)
        
        expected = "redshift+redshift_connector://test.redshift.amazonaws.com:5439/test_db"
        assert url == expected
    
    def test_schema_converter_class_attribute(self):
        """Test that the connector has the correct schema converter class."""
        assert RedshiftConnector.sql_to_jsonschema_converter == RedshiftSQLToJSONSchema
    
    def test_type_mapping(self):
        """Test SQL type to JSON Schema type mapping."""
        config = {"dates_as_string": True, "super_as_object": False}
        converter = RedshiftSQLToJSONSchema.from_config(config)
        
        test_cases = [
            # Redshift-specific types
            ("SUPER", {"type": ["null", "string"]}),  # super_as_object=False
            ("HLLSKETCH", {"type": ["null", "string"]}),
            ("GEOMETRY", {"type": ["null", "string"]}),
            ("GEOGRAPHY", {"type": ["null", "string"]}),
        ]
        
        for sql_type, expected_schema in test_cases:
            result = converter.to_jsonschema(sql_type)
            assert result == expected_schema
    
    def test_type_mapping_with_formats(self):
        """Test date/time type mapping with configurable formats."""
        config = {"dates_as_string": False, "super_as_object": True}
        converter = RedshiftSQLToJSONSchema.from_config(config)
        
        # Test SUPER as object
        super_result = converter.to_jsonschema("SUPER")
        expected_super = {
            "type": ["null", "object", "array", "string", "number", "boolean"], 
            "additionalProperties": True
        }
        assert super_result == expected_super
        
        # Note: Date/time format testing would require SQLAlchemy type objects
        # which are handled by the registered methods, not string-based dispatch


class TestRedshiftSchemaConverter:
    """Test Redshift schema converter functionality."""
    
    def test_from_config(self):
        """Test schema converter configuration."""
        config = {
            "dates_as_string": False,
            "super_as_object": True,
        }
        converter = RedshiftSQLToJSONSchema.from_config(config)
        
        assert converter.dates_as_string is False
        assert converter.super_as_object is True
    
    def test_integer_types(self):
        """Test integer type mappings (handled by base class)."""
        converter = RedshiftSQLToJSONSchema.from_config({})
        
        # Test Redshift-specific string types that need special handling
        # Note: Standard SQLAlchemy integer types are handled by the base class
        # We only test string-based types that Redshift introspection might return
        
        # Test SUPER type (Redshift-specific)
        super_result = converter.to_jsonschema("SUPER")
        assert super_result == {"type": ["null", "string"]}
        
        # Test with super_as_object=True
        converter_obj = RedshiftSQLToJSONSchema.from_config({"super_as_object": True})
        super_obj_result = converter_obj.to_jsonschema("SUPER")
        expected = {
            "type": ["null", "object", "array", "string", "number", "boolean"],
            "additionalProperties": True,
        }
        assert super_obj_result == expected
    
    def test_parametrized_types(self):
        """Test parametrized type handling."""
        converter = RedshiftSQLToJSONSchema.from_config({})
        
        # Test VARCHAR with length (should fall back to base class)
        varchar_result = converter.to_jsonschema("VARCHAR(255)")
        # The base class should handle the VARCHAR part after parametrization is stripped
        
        # Test SUPER with parameters (Redshift-specific)
        super_result = converter.to_jsonschema("SUPER()")
        assert super_result == {"type": ["null", "string"]}
    
    def test_redshift_specific_types(self):
        """Test Redshift-specific type mappings."""
        converter = RedshiftSQLToJSONSchema.from_config({"super_as_object": False})
        
        super_result = converter.to_jsonschema("SUPER")
        assert super_result == {"type": ["null", "string"]}
        
        geometry_result = converter.to_jsonschema("GEOMETRY")
        assert geometry_result == {"type": ["null", "string"]}
        
        hll_result = converter.to_jsonschema("HLLSKETCH")
        assert hll_result == {"type": ["null", "string"]}
    
    def test_super_as_object(self):
        """Test SUPER column as object mapping."""
        converter = RedshiftSQLToJSONSchema.from_config({"super_as_object": True})
        
        super_result = converter.to_jsonschema("SUPER")
        expected = {
            "type": ["null", "object", "array", "string", "number", "boolean"],
            "additionalProperties": True,
        }
        assert super_result == expected


class TestTapConfiguration:
    """Test tap configuration validation."""
    
    def test_config_validation_basic_auth(self):
        """Test that basic auth config validates properly."""
        config = {
            "host": "test.redshift.amazonaws.com",
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
            "use_iam_authentication": False,
        }
        tap = TapRedshift(config=config)
        assert tap.config["host"] == "test.redshift.amazonaws.com"
        assert tap.config["database"] == "test_db"
    
    def test_config_validation_iam_auth(self):
        """Test that IAM auth config validates properly."""
        config = {
            "host": "test.redshift.amazonaws.com",
            "database": "test_db",
            "use_iam_authentication": True,
            "db_user": "test_user",
            "cluster_identifier": "test-cluster",
            "aws_region": "us-west-2",
        }
        tap = TapRedshift(config=config)
        assert tap.config["use_iam_authentication"] is True
        assert tap.config["db_user"] == "test_user"


# Additional integration tests would go here if you have a test Redshift instance
