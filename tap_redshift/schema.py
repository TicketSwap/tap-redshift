"""SQL to JSON Schema conversion for Redshift."""

from __future__ import annotations

import functools
import typing as t

import sqlalchemy
from singer_sdk.connectors.sql import SQLToJSONSchema


class RedshiftSQLToJSONSchema(SQLToJSONSchema):
    """Custom SQL to JSON Schema conversion for Redshift."""

    def __init__(self, *, dates_as_string: bool = True, super_as_object: bool = False, **kwargs):
        """Initialize the SQL to JSON Schema converter.

        Args:
            dates_as_string: Whether to convert dates to strings instead of date format
            super_as_object: Whether to treat SUPER columns as objects instead of strings
            **kwargs: Additional arguments passed to parent class
        """
        super().__init__(**kwargs)
        self.dates_as_string = dates_as_string
        self.super_as_object = super_as_object

    @classmethod
    def from_config(cls, config: dict) -> RedshiftSQLToJSONSchema:
        """Instantiate the SQL to JSON Schema converter from a config dictionary.

        Args:
            config: Configuration dictionary

        Returns:
            Configured RedshiftSQLToJSONSchema instance
        """
        return cls(
            dates_as_string=config.get("dates_as_string", True),
            super_as_object=config.get("super_as_object", False),
        )

    @functools.singledispatchmethod
    def to_jsonschema(self, column_type: t.Any) -> dict:
        """Customize the JSON Schema for Redshift types.

        Args:
            column_type: SQLAlchemy column type

        Returns:
            JSON Schema dictionary
        """
        # Handle string-based type names (common in Redshift introspection)
        if isinstance(column_type, str):
            type_name = column_type.lower()

            # Redshift-specific types that aren't in the base class
            if type_name == "super":
                if self.super_as_object:
                    return {
                        "type": ["null", "object", "array", "string", "number", "boolean"],
                        "additionalProperties": True,
                    }
                return {"type": ["null", "string"]}
            elif type_name == "hllsketch":
                return {"type": ["null", "string"]}
            elif type_name in ("geometry", "geography"):
                return {"type": ["null", "string"]}

            # Handle parametrized types (e.g., varchar(255), decimal(10,2))
            elif "(" in type_name:
                base_type = type_name.split("(")[0]
                return self.to_jsonschema(base_type)

        # Fall back to the parent implementation for standard types
        return super().to_jsonschema(column_type)

    @to_jsonschema.register
    def numeric_to_jsonschema(self, column_type: sqlalchemy.types.Numeric) -> dict:
        """Override the default mapping for NUMERIC columns.

        For example, a scale of 4 translates to a multipleOf 0.0001.
        """
        if column_type.scale is not None and column_type.scale > 0:
            return {
                "type": ["null", "number"],
                "multipleOf": 10 ** (-column_type.scale),
            }
        return {"type": ["null", "number"]}

    @to_jsonschema.register
    def datetime_to_jsonschema(self, column_type: sqlalchemy.types.DateTime) -> dict:
        """Override the default mapping for DATETIME columns."""
        if self.dates_as_string:
            return {"type": ["null", "string"]}
        return {"type": ["null", "string"], "format": "date-time"}

    @to_jsonschema.register
    def date_to_jsonschema(self, column_type: sqlalchemy.types.Date) -> dict:
        """Override the default mapping for DATE columns."""
        if self.dates_as_string:
            return {"type": ["null", "string"]}
        return {"type": ["null", "string"], "format": "date"}

    @to_jsonschema.register
    def time_to_jsonschema(self, column_type: sqlalchemy.types.Time) -> dict:
        """Override the default mapping for TIME columns."""
        if self.dates_as_string:
            return {"type": ["null", "string"]}
        return {"type": ["null", "string"], "format": "time"}

    @to_jsonschema.register
    def timestamp_to_jsonschema(self, column_type: sqlalchemy.types.TIMESTAMP) -> dict:
        """Override the default mapping for TIMESTAMP columns."""
        if self.dates_as_string:
            return {"type": ["null", "string"]}
        return {"type": ["null", "string"], "format": "date-time"}

    # Redshift-specific type handlers would go here if we had custom SQLAlchemy types
    # For now, the string-based handler covers most Redshift introspection cases
