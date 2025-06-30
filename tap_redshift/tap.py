"""Redshift tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_redshift.client import RedshiftStream


class TapRedshift(SQLTap):
    """Redshift tap class."""

    name = "tap-redshift"
    default_stream_class = RedshiftStream

    config_jsonschema = th.PropertiesList(
        # Database connection settings
        th.Property(
            "host",
            th.StringType(nullable=False),
            required=True,
            title="Redshift Host",
            description="The hostname of the Redshift cluster",
        ),
        th.Property(
            "port",
            th.IntegerType(),
            default=5439,
            title="Port",
            description="The port of the Redshift cluster",
        ),
        th.Property(
            "database",
            th.StringType(nullable=False),
            required=True,
            title="Database",
            description="The name of the Redshift database",
        ),
        th.Property(
            "schemas",
            th.ArrayType(th.StringType()),
            title="Schemas",
            description="The schemas to extract tables from",
        ),
        # Authentication settings
        th.Property(
            "use_iam_authentication",
            th.BooleanType(),
            default=False,
            title="Use IAM Authentication",
            description="Whether to use IAM authentication instead of username/password",
        ),
        th.Property(
            "username",
            th.StringType(),
            title="Username",
            description="The username for database authentication (required if not using IAM)",
        ),
        th.Property(
            "password",
            th.StringType(),
            secret=True,
            title="Password",
            description="The password for database authentication (required if not using IAM)",
        ),
        # IAM/Serverless settings
        th.Property(
            "cluster_identifier",
            th.StringType(),
            title="Cluster Identifier",
            description="The cluster identifier (required for provisioned clusters with IAM)",
        ),
        # AWS credentials and settings
        th.Property(
            "aws_region",
            th.StringType(),
            default="eu-west-1",
            title="AWS Region",
            description="The AWS region of the Redshift cluster",
        ),
        # SSL settings
        th.Property(
            "ssl",
            th.BooleanType(),
            default=True,
            title="Use SSL",
            description="Whether to use SSL for the connection",
        ),
        th.Property(
            "sslmode",
            th.StringType(),
            default="require",
            title="SSL Mode",
            description="The SSL mode to use (require, prefer, allow, disable)",
        ),
        # S3 unload settings
        th.Property(
            "s3_bucket",
            th.StringType(),
            required=True,
            title="S3 Bucket",
            description="The S3 bucket to use for UNLOAD operations",
        ),
        th.Property(
            "s3_key_prefix",
            th.StringType(),
            title="S3 Key Prefix",
            description="The S3 key prefix for UNLOAD operations",
        ),
        th.Property(
            "copy_role_arn",
            th.StringType(),
            default="DEFAULT",
            title="Copy role ARN",
            description="Redshift copy role arn to use for the UNLOAD command from s3",
        ),
        # Schema conversion settings
        th.Property(
            "dates_as_string",
            th.BooleanType(),
            default=False,
            title="Dates as String",
            description="Whether to convert date/time columns to strings instead of date formats",
        ),
        th.Property(
            "super_as_object",
            th.BooleanType(),
            default=True,
            title="SUPER as Object",
            description="Whether to treat SUPER columns as objects instead of strings",
        ),
    ).to_dict()


if __name__ == "__main__":
    TapRedshift.cli()
